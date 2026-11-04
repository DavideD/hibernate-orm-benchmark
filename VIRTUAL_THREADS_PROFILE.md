# Virtual threads against platform threads: the profiles

This is the analysis of the recordings of `ORM_VS_REACTIVE.md` section 13: Hibernate ORM sessions on a
virtual thread each against a pool of 8 platform threads, 2 cores, 8 connections, JDK 25.0.4 and Netty 4.2.20.Final.
The follow-up experiments, and the conclusions, are in that section: in particular, the pool of virtual threads
(`executor=virtualPool`) that checks the role of Agroal.

The recordings were made by the `prof` runs described there: async-profiler 4.6 (`cpu` at 200 µs and `wall` at 1 ms) and the JDK's
Flight Recorder, 18 s of the measurement window each. They are not in git. They were analysed with the Jafar MCP server
(`jfr-mcp` 0.29.0), `jfrconv` and the JDK's `jfr` tool, and the Python scripts of `profile/virtual-threads/`
(they read the collapsed stacks of `jfrconv` and the output of `jfr print`).

All numbers come from those recordings: 18 s each, 2 CPUs (8-9), 8 connections. "Per session" divides by achievedRate × 18 s: about 269.5k sessions at 15000/s and 27.1k at 1500/s.
async-profiler cpu samples are 200 µs each, so per-session CPU = samples × 200 µs / sessions. Wall samples are 1 ms per thread.

Tools:
- `jfrconv` (async-profiler 4.6) turned the cpu/wall recordings into collapsed stacks with `-t`. The stacks were then grouped by the Python scripts in `profile/virtual-threads/` (`classify.py`, `incl.py`, `wallcls.py`).
- `jfr summary` and `jfr print` gave the JDK recordings (ThreadPark, SocketRead, Deoptimization, NativeMethodSample, GC); `park.py`, `sockread.py` and `timeline.py` parse them.
- jafar (`jfr_summary`, `jfr_compare`, `jfr_hotmethods`, `jfr_query`) was used for cross-checks. How well each jafar tool worked is at the end.

## TL;DR

1. **CPU at 15000/s.** The two executors cost about the same: 55.1 µs/session platform against 57.8 µs virtual in the profiled runs (54.7 against 51.3 in the unprofiled runs). Virtual threads save the blocking `poll()` per round trip (15.5 µs/session), part of `read()` (5.3 µs) and half of the context switches (5.6 → 2.9 per session). They add back about the same amount:
   - epoll_ctl registration: 4.3 µs
   - the poller virtual threads: 3.3 µs
   - park/unpark/freeze/thaw: about 4.4 µs
   - Agroal connection hand-off: 1.3 µs
   - the FJP **DelayScheduler**, because Agroal's wait is a *timed* park: 2.5 µs
   - the ForkJoinPool idle loop: 1.0 µs
2. **Tail and max rate.** The limit is not CPU. With virtual threads all sessions start at once and queue inside **Agroal** (LinkedTransferQueue + timed park) instead of in the ThreadPoolExecutor queue. Each database round trip goes through the poller and the FJP run queue, so it waits longer: SocketRead p99 0.75 ms against 0.105 ms. That wait lengthens the time each of the 8 connections is held.
   - The JFR run also caught a **1.03 s window with no socket I/O at all**: all 8 connections were idle while about 15000 sessions waited in Agroal. Only the expiry of Agroal's internal **1-second** `poll(timeout)` ended it.
   - The pattern matches a check-then-enqueue race in Agroal's `handlerFromSharedCache` / `returnConnectionHandler`, which strands CHECKED_IN connections while waiters exist. It cannot happen with 8 platform threads for 8 connections, because nobody ever waits in Agroal.
   - I think this is the main reason for p99/p99.9 spikes and probably a big part of the 18.7k limit. Confidence: high on the stall itself, medium on the exact race, medium-low that it is *the* throughput limiter.
   - GC, the `load-scheduler`, and the master poller use well under 1% of the CPUs at 15000/s, so they are not competing in any meaningful way.
3. **1500/s with a 1 ms delay.**
   - Virtual: 119 µs/session, 12.6 context switches/session. Platform: 83 µs and 7.7 (profile and perf).
   - Each round trip, a carrier goes idle: it spins in `ForkJoinPool.deactivate` (8.1 µs/session), parks, and is then woken through the chain master poller → sub-poller VT → session VT.
   - That is about 2 thread wake-ups and about 2.1 context switches per round trip against about 1.3 for a platform thread blocked in `poll()`.
   - On top of that: epoll_ctl, sub-poller epoll_wait and continuation work (about 12 µs/session).
4. **Other observations.**
   - The async-profiler `cpu` profile of the *platform* run under-attributes user-mode Java: Hibernate's `JdbcValuesResultSetImpl.<init>` has 2 samples there against 562 in the virtual run. The wall profile gives the same Hibernate/pgjdbc Java cost on both (6.3 against 6.0 µs/session).
   - Virtual threads allocate about 40% more per session: StackChunk is 12.5 KB of 37.6 KB.
   - jdk.ThreadPark does **not** record virtual-thread parks. The 571k events are the DelayScheduler thread (82%), carriers and load-scheduler.

---

## 1. Where the CPU goes at 15000/s (c2-p8-l0-r15000-*-cpu.jfr)

The two runs are comparable. jfr_compare: 74 232 samples against 77 895 over 18.0 s, "Recordings appear comparable". The benchmark's own numbers for these runs: 57.1 against 56.3 µs CPU/request.

Exclusive categories, first match wins (`classify.py`, from jfrconv collapsed stacks). Values are µs per session:

| category | platform | virtual | Δ |
|---|---:|---:|---:|
| socket `write0` (send + loopback softirq rx into PG + PG wake-up) | 21.34 | 24.22 | +2.9 |
| socket `read0` | 14.65 | 9.38 | −5.3 |
| blocking `Net.poll` → `do_sys_poll` (platform only) | 15.50 | 0 | −15.5 |
| `Poller.register/deregister` → `EPoll.ctl` (session VT before parking) | 0 | 4.33 | +4.3 |
| poller VTs (`Poller.subPollerLoop`: epoll_wait, wakeup, yield, re-arm) | 0 | 3.34 | +3.3 |
| `VirtualThread.unpark` / `submitRunContinuation` / `signalWork` / `Unsafe.unpark` | 0.08 | 1.69 | +1.6 |
| `VirtualThread.park` / `yieldContinuation` / freeze | 0 | 1.00 | +1.0 |
| VT mount/thaw/`runContinuation` (`cont_thaw`, `enterSpecial`) | 0 | 1.73 | +1.7 |
| Agroal `getConnection`/`returnConnection` (scan, LTQ poll/tryTransfer) | 0.00 | 1.25 | +1.3 |
| FJP worker loop (`deactivate`, `awaitWork`, `scan`) | 0 | 1.00 | +1.0 |
| TPE `getTask` / LinkedBlockingQueue | 0.07 | 0 | −0.1 |
| `ForkJoinPool-1-delayScheduler` thread | 0 | 0.40 | +0.4 |
| MasterPoller thread | 0 | 0.09 | +0.1 |
| load-scheduler thread | 0.01 | 0.05 | 0 |
| GC threads / JIT threads | 0.17 / 0.02 | 0.21 / 0.09 | ~0 |
| session Java (Hibernate + Agroal + pgjdbc, rest of stack) | 3.23 (*) | 8.91 (*) | (*) see §4.1 |
| **total** | **55.1** | **57.8** | +2.7 |

Inclusive cross-cuts (`incl.py`), virtual run:
- Everything related to timed parks: `DelayScheduler.*` on carriers plus the delayScheduler thread = 2.50 µs/session. Most of it, 1.77 µs, is `VirtualThread.cancelTimeoutTask → DelayScheduler.pend → signal → Unsafe.unpark → futex_wake`.
- `try_to_wake_up` 1.12 µs, futex 1.88 µs, `clock_gettime` 0.32 µs.
- The timed parks come from Agroal: `ConnectionPool.waitAvailableHandler`/`handlerFromSharedCache → LinkedTransferQueue.poll(timeout) → LockSupport.parkNanos → VirtualThread.parkNanos`. These are the only `parkNanos` callers on virtual threads in the cpu, wall and 1500/s profiles.

**What virtual threads save:**
- The blocking `poll()` that `NioSocketImpl` does for a platform thread after an EAGAIN read: `Net.poll` is 28% of platform CPU (15.5 µs/session), with `do_sys_poll`, `poll_schedule_timeout`, `__pollwait` and `tcp_poll` at the top of jfr_compare's "improvement" list.
- About 5 µs of read path.
- Half the context switches: perf 1 520 853 → 783 638 in 18 s, i.e. 5.64 → 2.91 per session. CPU migrations drop from 47 297 to 19 664.
- The platform hand-off itself is not expensive: TPE/LBQ is only 0.07 µs/session on CPU. The platform threads do park in `LinkedBlockingQueue.take` 0.49 times per session (jdk.ThreadPark, p50 0.59 ms), and those parks are part of the 5.6 context switches.

**What virtual threads add:**
- epoll_ctl: 2 per round trip, the session's oneshot registration and the sub-poller re-arming itself on the master. `__GI_epoll_ctl` and `do_epoll_ctl_file` are new in jfr_compare.
- The sub-poller loop (`EPoll.wait` 4.5% self).
- Continuation freeze/thaw (`memcpy` 1.26% self).
- unpark/signalWork.
- The DelayScheduler round trips for every Agroal wait.
- Agroal's contended path: `AbstractQueuedSynchronizer.signalNext` 1.12%, `LinkedTransferQueue.tryTransfer`, `handlerFromSharedCache` 0.71 µs.

With platform threads, Agroal is free: 8 threads hold 8 connections and its thread-local `LocalConnectionCache` always hits (0 samples in Agroal's slow paths). With a new virtual thread per session, the local cache always misses.

ForkJoinPool scan/awaitWork, Continuation run/yield, unpark, the pollers, epoll_ctl and clock reads add up to about 15 µs/session. The removed `poll()` and read work is about 21 µs/session. That is why the unprofiled virtual run is 3.4 µs/session *cheaper*.

Confidence: high on the categories and the signs. ±1-2 µs on individual rows, because a single run per mode and attribution skew (§4.1) move a few µs between "Java" and "syscall".

## 2. Why the tail is worse and the max rate stops at 1.1 busy cores

### 2.1 Where sessions and carriers wait

jdk.ThreadPark (`jfr print`, `park.py`). Note that **virtual-thread parks are not recorded as jdk.ThreadPark**. Only platform threads appear:

| recording | thread / site | events | /session | p50 | p90 | p99 |
|---|---|---:|---:|---:|---:|---:|
| platform 15k | request-N, `LinkedBlockingQueue.take` | 132 418 | 0.49 | 0.59 ms | 0.76 | 0.90 |
| platform 15k | load-scheduler `parkNanos` | 16 952 | 0.06 | 1.05 ms | | |
| virtual 15k | **ForkJoinPool-1-delayScheduler**, timed | 466 280 | **1.73** | 15 µs | 23 µs | 56 µs |
| virtual 15k | delayScheduler, untimed | 14 862 | 0.06 | 0.61 ms | | |
| virtual 15k | carriers `ForkJoinPool.awaitWork` (2 stack variants) | 72 954 | 0.27 | 19-28 µs | 0.6-1.05 ms | 1.3-2.4 ms |
| virtual 15k | load-scheduler | 16 867 | 0.06 | 1.05 ms | | |

- 82% of the 571k ThreadPark events are the DelayScheduler thread, woken about 26 700 times/s by Agroal's timed waits: `schedule` when a VT parks, `cancel` when it is unparked.
- The delayScheduler alone accounts for about 1.8 of the 2.9 context switches per session in the virtual run. Its "next deadline" (the ThreadPark `timeout` field) is 30 s for 62% of its timed parks, which is Agroal's acquisitionTimeout. It is 0.5-1 s for 37%, which is Agroal's 1 s fairness poll (see 2.3).
- Carriers park 0.27 times per session. Half of those parks last less than about 25 µs, so carriers go to sleep and are woken almost immediately (churn). Their total parked time is 16.8 s of 36 s carrier time.

Wall-clock profile (`wallcls.py`, c2-p8-l0-r15000-*-wall.jfr):
- Platform request-N threads: 42.6% idle in `getTask`, 51.2% blocked in `Net.poll` waiting for PG, 3.5% write0, 1.4% read0, 1.3% Java.
- Virtual carriers: **65.2% parked in `awaitWork`**, 16.2% write0, 5.6% read0, 4.7% session Java, 2.2% epoll_ctl, 2.2% poller VTs, 2.0% unpark, 1.1% thaw.
- So at 15000/s the carriers are idle two thirds of the time.

**Is there evidence that the GC, the load-scheduler or the master poller compete with the carriers?** Very little:
- GC threads: 0.21 µs/session (0.4% of samples), pauses ≈1 ms every ≈2 s.
- load-scheduler: 0.05 µs/session.
- MasterPoller: 0.09 µs/session. Under load the sub-pollers mostly `Thread.yield` and re-poll instead of parking on the master.
- delayScheduler: 0.40 µs/session of CPU, but 26.7k wake-ups/s on the same 2 CPUs.

Its wake-ups are the only "competitor" of any significance. Each one is a futex wake from a carrier (`cancelTimeoutTask → … → try_to_wake_up`, 1.8 µs/session) and a context switch on one of the two CPUs.

The recordings contain no run-queue latency data (no sched events), so "carrier preempted while VTs are queued" cannot be measured directly.

### 2.2 Per-round-trip latency on the connection

jdk.SocketRead (threshold 0, throttled to about 290 events/s by the 300/s throttle in the active settings). jafar's `quantiles` gives the same values as `sockread.py`:

| 15000/s | n | p50 | p90 | p99 | p99.9 | >0.2 ms | >0.5 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| platform | 5206 | 39 µs | 60 µs | **105 µs** | 315 µs | 0.27% | 0.06% |
| virtual | 4992 | 36 µs | 70 µs | **754 µs** | 1.30 ms | 2.84% | 1.90% |

The read latency includes the time from "data arrived" until the reader runs again:
- Platform: one kernel wake-up of the thread blocked in `poll()`.
- Virtual: the sub-poller VT has to be run (or the master poller woken first), it unparks the session VT onto a carrier queue, and the session VT waits there behind whatever else is runnable. That includes new sessions that the load-scheduler submits in bursts once per millisecond, which run until they park in Agroal.

The median is the same, but 1 read in 50 waits more than 0.5 ms with virtual threads. Each session does 5 reads while holding a connection, so this goes straight into connection hold time.

By Little's law, the pool caps throughput at 8 / hold time: 32.8k/s means about 244 µs per session for platform, 18.7k/s means about 428 µs for virtual. The extra ≈180 µs per session is scheduling and hand-off latency, not CPU: the max-rate log shows 50.6 µs CPU/request at 18.7k, *less* than platform.

The max-rate logs also show that virtual threads *can* complete 25.6k/s (99.5% completed at 1.74 busy CPUs). They just do it with p50 of 11-17 ms. Latency grows long before the CPUs saturate.

### 2.3 The 1-second stall in the virtual JFR run (c2-p8-l0-r15000-virtual-jfr.jfr)

That run reports p99 = 1.34 s, p99.9 = 1.95 s and maxInFlight = 15 022. The timeline (`timeline.py` over ThreadPark, SocketRead, SocketWrite, NativeMethodSample, Deoptimization):

- **15:59:09.864**: last SocketRead. The next one is at **15:59:10.894**, a 1.03 s gap. SocketWrite has the same gap (09.852 → 10.879).
- During the gap:
  - Each carrier parks about 95 times per 100 ms, i.e. once per load-scheduler batch: it wakes, runs the new sessions, which immediately block in Agroal, and parks again.
  - The delayScheduler parks about 300 times per 100 ms (timers for the new waiters).
  - Every NativeMethodSample is `MasterPoller` in `EPoll.wait`. No VT is in a socket read or write.
  - So all 8 connections are idle while sessions pile up.
- **15:59:10.868**: 128 deoptimizations, all `unstable_if` / `class_check` on branches **never taken before in the run**:
  - `DelayScheduler.loop` line 317, `if (... d > 0L)`: the first time a scheduled timeout actually **expired**;
  - `VirtualThread.unpark` line 857 (unpark from a timeout);
  - `ForkJoinTask.trySetCancelled`;
  - 55 × `LinkedTransferQueue.poll(long, TimeUnit)` and 55 × `LinkedTransferQueue$DualNode.await`: the parked frames of Agroal waiters whose poll returned on timeout.
- **15:59:10.879**: I/O resumes. From 11.4 to 12.9 the carriers never park while they drain the backlog. A 25 ms young GC at 11.55 follows the heap growth of about 15k parked VTs.

Agroal 3.0.1 bytecode (`javap` on ConnectionPool), `handlerFromSharedCache`:
- If `handlerTransferQueue.hasWaitingConsumer()` is true, a new caller waits in line with `poll(min(ONE_SECOND, 0.9 × acquisitionTimeout))`, i.e. **1 s**, *without* scanning the connection list.
- Otherwise it scans `allConnections` and, if none is free, goes to `waitAvailableHandler` → `poll(remaining≈30 s)`.

`returnConnectionHandler` sets the connection CHECKED_IN and then `tryTransfer`s it. `tryTransfer` only succeeds if a consumer is *already* waiting.

The race: a session's scan fails, then all in-use connections are returned before that session is actually enqueued in the LTQ. The returned connections stay CHECKED_IN. From then on every newcomer sees a waiting consumer and queues for 1 s instead of scanning. Nobody holds a connection, so nobody returns one, so nothing is transferred, until the first 1 s poll times out and that waiter scans the list.

That matches the 1.03 s gap, the first-ever timer expiry and the deoptimization pattern exactly.
- With 8 platform threads for 8 connections nobody ever waits in Agroal, so this cannot happen.
- With virtual threads there are hundreds of waiters, and the carriers can be preempted between "scan failed" and "enqueued" (2 CPUs shared with the delayScheduler, master poller, GC and JIT).
- A partial version of the same race (some connections stranded while the waiter queue never empties) would shrink the effective pool under sustained load. That would explain latency rising at 1.1-1.3 busy cores in the max-rate search, and the step `max-virtual-pollerMode1-r1 @17086: p50 601 ms, p99 3.1 s, completed 106.9%`.

The recordings cannot prove the partial version: there is no pool-level metric. To test it, enable Agroal metrics (available vs. awaiting count) or count LTQ.poll timeouts in a dedicated run.

### 2.4 Why 4 carriers on 2 cores help (+40%), and why pollerMode=1 does not

This is inferred from the data above, not measured directly (there is no recording of those configurations):
- The latency added per round trip is time spent *queued for a carrier*: SocketRead p99 0.75 ms, and VTs holding connections wait behind new sessions and poller VTs.
- With parallelism = 4 there are 4 kernel threads to multiplex the runnable VTs on 2 CPUs. When one carrier is preempted (delayScheduler wake-ups, master poller, GC, JIT) or is busy with a burst of new sessions, another carrier can run the VT that holds a connection. The kernel scheduler also gives freshly woken threads quick access to a CPU. In effect this brings back some of the preemptive, wake-up-driven scheduling that platform threads get for free.
- It also shortens the window in which a scanning VT can be descheduled before it enqueues in Agroal (2.3), though that part is speculative.
- pollerMode=1 (platform read pollers) adds 2 more platform threads that must be woken for every event and then hand off to a carrier. That is one more cross-thread wake-up per round trip, so it gets worse (16.7k, and 64-66 µs CPU/request against 50 in the logs).
- With both settings (20.8-22.4k) the extra carriers help, but the cost of the platform poller remains.

## 3. 1500/s with a 1 ms `pg_sleep` (c2-p8-l1-r1500-*)

perf, unprofiled ("none") runs: platform 7.67 context switches/session, virtual 12.59. Profiled cpu runs: 82.6 against 119.4 µs/session (`classify.py`):

| category | platform | virtual | Δ |
|---|---:|---:|---:|
| write0 (incl. loopback softirq into PG, PG wake-up) | 50.37 | 65.53 | +15.2 |
| read0 | 15.41 | 8.30 | −7.1 |
| blocking `Net.poll` | 8.01 | 0 | −8.0 |
| **FJP idle loop**: `deactivate` spin 8.09 + `awaitWork`/park 2.57 + `Thread.interrupted`/`getAndClearInterrupt` 2.04 | 0 | 12.15 | +12.2 |
| epoll_ctl registration (session) | 0 | 6.41 | +6.4 |
| poller VTs: epoll_wait 2.33, re-arm on master 1.64, yield 0.43 | 0 | 4.40 | +4.4 |
| MasterPoller thread | 0 | 1.35 | +1.4 |
| VT park/freeze 1.31, mount/thaw 1.39, unpark 0.33 | 0 | 3.03 | +3.0 |
| Agroal | 0.06 | 0.92 | +0.9 |
| JIT threads | 2.74 | 5.03 | +2.3 |
| session Java (*) | 4.76 | 8.59 | (*) |
| total | 82.6 | 119.4 | +36.9 |

jdk.ThreadPark per session at 1500/s:
- Platform: request-N `LinkedBlockingQueue.take` 0.94, load-scheduler 0.60.
- Virtual: **carriers 6.54** (94 294 + 83 065; p50 20-30 µs), load-scheduler 0.60, delayScheduler 0.32.

The master poller blocks in epoll_wait, which is not a park, so it is not counted.

There are 6 round trips per session (pg_sleep + 5 finds).

**Platform thread, per round trip:** `write`, then `read` → EAGAIN, then `poll()` blocks. PG answers, the kernel wakes *this* thread (1 wake-up, 1 context switch), and it reads. That gives about 6 poll blocks + 0.94 LBQ parks + 0.6 load-scheduler parks ≈ 7.5 context switches/session, against the 7.67 measured.

**Virtual thread, per round trip:**
1. The session VT does `write`, then `read` → EAGAIN, then `Poller.register` (epoll_ctl on the sub-poller's epfd), then parks (freeze).
2. The sub-poller VT finds nothing, re-arms its epfd on the master poller (another epoll_ctl) and parks.
3. The carrier has nothing left. It spins in `deactivate` (up to max(3n, 256) `onSpinWait`), then parks in `awaitWork`: context switch.
4. PG answers. The **MasterPoller** platform thread wakes from epoll_wait (context switch) and unparks the sub-poller VT, an external submission → `signalWork` → futex wake of a **carrier** (context switch).
5. The carrier thaws the sub-poller. It runs `epoll_wait(0)`, unparks the session VT and `Thread.yield`s.
6. The session VT is thawed and reads. Next time round, the sub-poller polls 0 events and re-arms (back to step 2).

That is 2 wake-ups of sleeping kernel threads per round trip instead of 1, plus 2 epoll_ctl, 2-3 epoll_wait and 3 continuation mounts. The count matches: carriers 6.54 + load-scheduler 0.6 + delayScheduler 0.32 + MasterPoller ≈ 5 (the remainder of 12.59) → about 2.1 context switches per round trip against about 1.3 for platform threads.

When the CPUs are mostly idle, the carrier spin before each park and the idle-loop bookkeeping (12 µs/session) are pure overhead. So are epoll_ctl, the sub-poller and the master poller (12 µs).

write0 is also 15 µs/session more expensive. That is mostly the softirq path that delivers the request into PG on the sender's CPU (`do_softirq`/`net_rx_action` 30.4 → 43.2 µs). The likely cause is colder caches and deeper idle states on CPUs that sleep 6.5 times per session, but that is a guess: the recordings cannot separate it.

Latency is the same in both modes (p50 2.85 ms, p99 3.6 ms), because the pool never runs out at this rate.

Confidence: high on the wake-up chain and the counts, medium on the write0 explanation.

## 4. Other observations

### 4.1 cpu-mode attribution skew (affects §1's "session Java" row)
- In the **platform** cpu profile only 7.5% of samples have a Java leaf, against 21.8% in the virtual profile. `__syscall_cancel_arch` (14.8%), `do_syscall_64` (6.6%) and `_raw_spin_unlock_irqrestore` (10.2%) collect a lot of samples.
- jfr_compare shows `JdbcValuesResultSetImpl.<init>` 2 → 562 samples, `IdentityHashMap.get` 1 → 456, `HashMap.removeNode` 0 → 432. Those ratios are not physically plausible for identical code.
- The JDK recordings point the same way: ExecutionSample 21 (platform) against 185 (virtual).
- The async-profiler **wall** profiles give the same Java self time per session for both: 1706 against 1618 ms of samples, i.e. 6.3 against 6.0 µs/session.

Conclusion: in this container the platform cpu profile pushes user-mode Java samples into the following syscall entry. Treat Java-level differences between the two cpu profiles as unreliable. The kernel and VT-runtime categories are large enough not to be affected.

The box is an LXC container on bare metal (systemd-detect-virt = lxc), so this is not VM skid. I did not identify the mechanism.

### 4.2 Allocation
jdk.ObjectAllocationSample weights (jafar `sum(weight)`):
- 15000/s: platform 27.1 KB/session, virtual 37.6 KB/session. `jdk.internal.vm.StackChunk` alone is 12.5 KB/session: frozen stacks of VTs parked in Agroal with the deep Hibernate → Agroal → LTQ stack.
- 1500/s: 40.3 against 51.9 KB, of which StackChunk is 4.9 KB.
- These are estimates from sampled weights.

### 4.3 Other details
- JFR `jdk.SocketRead`/`SocketWrite` are throttled (about 290 events/s). jdk.ThreadPark is not throttled. jdk.ThreadPark does not cover VTs, so "group ThreadPark by Agroal / pgjdbc / poller" is impossible for virtual threads. Their waits only show through the delayScheduler (Agroal's timed waits) and the cpu/wall stacks.
- Agroal's waiting goes through `LinkedTransferQueue$DualNode.await`, which spins (`checkForUniprocessor`) before parking. That is small (0.04 µs/session).
- The JFR run itself perturbs the virtual case: it stalled, and its p99 was 1.3 s against 2.3 ms unprofiled. The async-profiler runs are closer to unprofiled: virtual-cpu p99 6.6 ms, virtual-wall 3.3 ms.

## 5. Tools: what worked, where they agree

**jafar (jfr-mcp 0.29.0):**
- `jfr_open`, `jfr_summary`: worked. The summary's event counts match `jfr summary`.
- `jfr_compare`: worked, and gave the useful differential above. Limitations:
  - it only compares self-time *shares*, not absolute µs/session;
  - its `type` labels are wrong for kernel frames (`do_syscall_64`, `tcp_poll` are labelled "java"), so `categoryBreakdown` is meaningless for async-profiler recordings with kernel frames.
- `jfr_hotmethods`: worked. Its values match the jfrconv self counts exactly: `__syscall_cancel_arch` 11 011, `_raw_spin_unlock_irqrestore` 7 566.
- `jfr_query`:
  - `groupBy`, `sum`, `quantiles` and filters worked on simple paths. SocketRead quantiles match `jfr print`: p99 753.6 µs against 754 µs. The output labels the 0.999 quantile as "p100".
  - **On the two-chunk virtual JFR file, counts are short**: `count()` gives 554 710 against 570 963 from `jfr summary` (and from jafar's own `jfr_summary`), and `groupBy(eventThread/javaName)` gives 563 607. Single-chunk files match exactly (platform 149 374; virtual 1500/s 202 124).
  - `groupBy(stackTrace/frames/3/...)` failed with "Error occurred while parsing JFR recording", while `frames/0` and `frames/2` worked.
  - `select("${...}")` string templates did not resolve nested paths (they came back as `". < ."`).
  - On async-profiler wall recordings the thread field is `sampledThread`, not `eventThread` (the error message says so).
- `jfr_flamegraph` with `direction=top-down, maxDepth=4`: returned leaf-side frames (`tcp_v4_do_rcv;…;_raw_spin_unlock_irqrestore`) rather than roots, so it looks truncated from the wrong end. I did not rely on it.
- Not tried: `jfr_use`, `jfr_stackprofile`, `jfr_tsa`, `jfr_diagnose`.

**Where the tools agree and where they don't:**
- jfrconv and jafar agree on sample totals and self counts.
- The JDK `jfr` tool and jafar agree on single-chunk event counts, and disagree by 2.8% on the two-chunk file (jafar is short).
- async-profiler cpu and the JDK ExecutionSample agree on the platform/virtual Java skew. The wall profile disagrees with both and is the more credible (§4.1).
- perf context-switch counts are consistent with the ThreadPark + poll counts (§3).

# Why the event loops use more CPU on Vert.x 5

Section 11 of `POOL_STEALING.md` found that every request costs much more event loop CPU
on Vert.x 5 than on Vert.x 4.5, with or without stealing.
This document profiles both stacks with async-profiler and finds out where the CPU goes.

## 1. Summary

- The extra CPU is **not** in Vert.x, the SQL client or Hibernate Reactive code.
  It is the event loop polling the selector in a loop.
- With Vert.x 5.1.10, Netty 4.2.19 (`NioIoHandler`) calls `Selector.selectNow()` again and again
  (79 `epoll_wait` per request instead of 10) whenever a scheduled task is less than 0.5 ms away.
  Vert.x timers are Netty scheduled tasks, so any event loop with a pending timer does it.
- Netty 4.1.139 (Vert.x 4.5) rounds that wait **up** to a whole millisecond and sleeps.
  Netty 4.2 rounds to the **nearest** millisecond, so a deadline < 0.5 ms away means "don't sleep".
- This explains 91% of the extra CPU in the benchmark (the selector poll, the clock reads and the loop around them).
  Reads, writes, the codec, the pool, Mutiny and Hibernate cost the same on both stacks (section 5).
- A one line change in Netty (`+ 500_000L` to `+ 995_000L` in `NioIoHandler.millisBeforeDeadline`)
  brings Vert.x 5 back to the Vert.x 4.5 CPU: 297 µs to 103 µs per request, against 102 µs.
- It reproduces without Hibernate (a Vert.x PostgreSQL pool, 261 µs against 82 µs per request)
  and without Vert.x (a Netty event loop with a timer, 28% of a core against 1.1%).
- The native epoll transport doesn't have the problem (83 µs).
- The prepared statement cache is a separate effect: it halves the CPU on both stacks,
  and the gap stays (107 µs against 58 µs per request with the cache on).
  The gap is not about statement preparation.

## 2. Method

The benchmark is `OpenLoopStealBenchmark` (`POOL_STEALING.md` section 6), unchanged except for a new option,
`cachePreparedStatements` (default `false`, as in the earlier results).

- Load: 4000 requests/s, 5 `find`s per request, 4 event loops, `poolSize=4`, `poolEventLoopSize=4`, 10 s warm-up, 20 s measured.
- Pools: `shared` (Quarkus-like, steals) and `perEventLoop` (never steals).
  Both stacks get exactly the same options.
- Stacks: `v4` is Hibernate Reactive 3.4.6 + Vert.x 4.5.35 + ORM 7.4.13,
  `vertx5` is Hibernate Reactive 4.5.11 + Vert.x 5.1.10 + ORM 7.4.13.
- Profiler: async-profiler 4.6, attached with `asprof start` when the benchmark prints `Measuring for`
  and stopped one second before the end, so the warm-up is not in the recording.
  Event `cpu-clock` (perf_events software clock), interval 200 µs, JFR output, `-XX:+DebugNonSafepoints`.
  `perf_event_paranoid` is `-1` here, so **`cpu-clock` worked and no fallback was needed**.
  Kernel frames are included.
- Repetitions: every configuration was profiled twice (`cpu-*-r1`, `-r2`) and also run twice without the profiler (`none-*`).
  The CPU per request in sections 3, 7 and 8 is from the runs without the profiler, `ThreadMXBean` thread CPU of the event loops
  divided by the completed requests, like the earlier results.
  The two runs were within 1–5% of each other (the spread is in the tables).
- Allocation: one `alloc` recording of each stack and pool (`--alloc 512k`).
- Analysis: `jfrconv --cpu --norm --dot -t -o collapsed` (one collapsed stack file per recording, split by thread)
  and `profile/analyze.py` to normalise by completed requests, group by package and subtract.
  Only the `vert.x-eventloop-thread-*` threads are counted (99% of the samples).
  Samples times 200 µs divided by the completed requests is the profile CPU per request.
- mcp-jafar (`btraceio/jafar`, `jfr-mcp` 0.29.0, run with JBang) was installed and registered
  (`claude mcp add jafar -- jbang jfr-mcp@btraceio --stdio`). It opened the recordings and
  `jfr_hotmethods` and `jfr_compare` agree with the numbers below (sample counts 43988 against 125643, a ratio of 2.86;
  the collapsed stacks give 2.88). Its output is in `/tmp/profile/analysis/mcp-jafar.txt`.
  The tables in this document come from `analyze.py`, because it does the per-request normalisation and the grouping.

### Environment

| | |
|---|---|
| CPU | AMD Ryzen 9 7950X3D (16 cores, 32 threads), 34 GB |
| Kernel | Linux 7.2.10 (Fedora 44), container, `perf_event_paranoid=-1` |
| JDK | OpenJDK 25.0.4.1 (Red Hat), `-Xms1g -Xmx1g` |
| JVM CPUs | `taskset -c 8-15` (8 cores, no SMT siblings in use) |
| PostgreSQL | 18 in rootless podman, `--cpus 3`, pinned to CPUs 0–3 |
| Gradle | 9.4.1 |
| async-profiler | 4.6 (sha256 of the download checked against the GitHub release) |

### Versions

| | `v4` | `vertx5` |
|---|---|---|
| Hibernate Reactive | 3.4.6 | 4.5.11 |
| Vert.x | 4.5.35 | 5.1.10 |
| Netty | 4.1.139.Final | 4.2.19.Final |
| Event loop | `NioEventLoop` | `SingleThreadIoEventLoop` + `NioIoHandler` |
| Transport | NIO (JDK `EPollSelectorImpl`) | NIO (JDK `EPollSelectorImpl`) |
| ORM | 7.4.13 | 7.4.13 |

Both use the NIO transport: Vert.x doesn't use the native transport unless `preferNativeTransport` is set,
and the native jars are not on the class path.
The profiles show `sun.nio.ch.EPollSelectorImpl` and `EPoll.wait` on both.

## 3. CPU per request

Event loop CPU per request, 4000 requests/s, 2 runs each without the profiler (average, the range is in brackets).
Sleeps are the voluntary context switches of the event loops per request.

| Pool | Prepared statement cache | `v4` | `vertx5` | `vertx5` + Netty patch (section 8) |
|---|---|---|---|---|
| `perEventLoop` | off | 102 µs (102–102), 10.4 sleeps | **297 µs** (296–297), 7.4 sleeps | 103 µs (103–104), 10.4 sleeps |
| `perEventLoop` | on | 58 µs (58–58), 5.6 sleeps | **107 µs** (104–111), 5.2 sleeps | 60 µs (60–61), 5.6 sleeps |
| `shared` | off | 133 µs (132–134), 70% stolen | **382 µs** (371–392), 58% stolen | 150 µs (150–151), 70% stolen |
| `shared` | on | 82 µs (82–83), 58% stolen | **207 µs** (206–207), 50% stolen | 97 µs (96–98), 59% stolen |

What the numbers show:

- Vert.x 5 is 2.9 times slower with the cache off and 1.8 times with it on (`perEventLoop`).
  With the cache on, half of the CPU per request is gone on both stacks,
  and what is left of Vert.x 5 is still 49 µs more.
  So the gap exists with and without prepared statement caching, and caching doesn't explain it.
- The same stack sleeps less per request on Vert.x 5 (7.4 against 10.4).
  The loops that spin don't go to sleep.
  The Vert.x 5 runs also steal less (58% against 70%), because the loops are rarely idle.
- The median latency is 0.3 ms lower on Vert.x 5 (1449 µs against 1749 µs, `perEventLoop`, cache off).
  The latency includes the start delay of the 1 ms timer, which Netty 4.1 rounds up and Netty 4.2 doesn't.
  The spin buys timers that are more precise. That is the trade-off of the Netty change (section 6).
- With the Netty patch the Vert.x 5 numbers are the Vert.x 4.5 ones, within 1–3 µs, for `perEventLoop`
  and 15–17 µs higher for `shared` (section 5).

## 4. Where the samples are

Event loop CPU per request from the `cpu-clock` profiles, `perEventLoop`, cache off (2 recordings per stack).
The profiler adds 4–12% (114 µs against 102 µs for `v4`, 309 µs against 297 µs for `vertx5`),
so use the section 3 table for absolute values and these tables for the shares.

**By phase of the event loop iteration:**

| Phase | `v4` | `vertx5` | Difference |
|---|---|---|---|
| Selector poll (`select`, `selectNow`) | 17.4 | 155.5 | **+138.1** |
| Loop around it (`reportActiveIoTime`, clock reads, `runIo`, ...) | 0.8 | 40.3 | **+39.5** |
| Tasks (`runAllTasks`) | 7.6 | 18.7 | +11.0 |
| I/O: read, decode, callbacks (`processSelectedKeys`) | 88.0 | 94.9 | +6.9 |
| Total | 113.9 | 309.4 | +195.5 |

With the cache on (`perEventLoop`): selector poll 4.5 → 40.5 (+36.0), loop 0.4 → 7.9 (+7.5),
tasks 7.8 → 9.5 (+1.8), I/O 59.0 → 57.8 (**−1.2**). Total 71.7 → 115.8.

With `shared`, cache off: selector poll +214.7, loop +46.5, I/O +17.5, tasks −1.9. Total 109.4 → 386.2.

**By package, self time (the frame that burns the CPU):**

| Package of the leaf frame | `v4` | `vertx5` | Difference |
|---|---|---|---|
| native (libc `syscall`, `clock_gettime`, vDSO) | 13.8 | 132.7 | +118.9 |
| kernel (syscalls, epoll, TCP) | 61.2 | 118.4 | +57.1 |
| JDK (`sun.nio.ch`, `CompletableFuture`, ...) | 20.6 | 34.9 | +14.4 |
| `io.netty` | 5.5 | 12.4 | +6.9 |
| `org.hibernate.reactive` | 5.9 | 3.8 | −2.1 |
| `io.vertx.core` | 2.8 | 2.1 | −0.7 |
| `io.smallrye.mutiny` | 1.7 | 1.2 | −0.5 |
| `io.vertx.sqlclient` | 1.4 | 0.9 | −0.5 |
| `io.vertx.pgclient` | 0.3 | 0.6 | +0.3 |
| `org.hibernate` (ORM) | 0.3 | 0.7 | +0.4 |

(Per request, in µs, of the profile. The first two rows are the `epoll_wait` syscalls and the `clock_gettime` calls of the spin.)

What the numbers show:

- Everything the application and the libraries do (Hibernate, Mutiny, the SQL client, the codec, the Vert.x contexts)
  uses the same or less CPU on Vert.x 5.
- The extra CPU is syscalls (`epoll_wait`, `clock_gettime`) and the Netty and JDK code around them.

## 5. The differential: Vert.x 5 minus Vert.x 4.5

Inclusive time of the frames that carry the difference, µs per request, `perEventLoop`, cache off:

| Frame | `v4` | `vertx5` | Difference |
|---|---|---|---|
| `NioEventLoop.select` (4.1) / `NioIoHandler.select` (4.2) | 17.4 | 155.5 | +138.1 |
| `sun.nio.ch.SelectorImpl.selectNow` | 0.1 | 136.9 | **+136.7** |
| `sun.nio.ch.EPollSelectorImpl.doSelect` | 17.0 | 141.7 | +124.6 |
| `sun.nio.ch.EPoll.wait` | 16.1 | 130.6 | +114.6 |
| `epoll_wait` (libc) | 16.0 | 123.1 | +107.1 |
| `os::javaTimeNanos` / `clock_gettime` (vDSO) | 1.8 | 50.1 | +48.3 |
| `io.netty.util.concurrent.SystemTicker.nanoTime` | 0.0 | 16.2 | +16.2 |
| `SingleThreadEventExecutor.reportActiveIoTime` | 0.0 | 10.1 | +10.1 |
| `EPollSelectorImpl.processUpdateQueue` + `SelectorImpl.processDeregisterQueue` | 0.9 | 9.3 | +8.4 |
| `SingleThreadEventExecutor.runAllTasks` | 7.6 | 18.7 | +11.0 |
| `NioEventLoop.processSelectedKeys` (4.1) / `NioIoHandler.processSelectedKeys` (4.2) | 88.0 | 94.9 | +6.9 |
| `sun.nio.ch.SocketDispatcher.write0` | 57.3 | 73.0 | +15.7 |
| `io.vertx.core.impl.ContextImpl.emit` (4.5) / `ContextBase.emit` (5) | 92.9 | 95.9 | +3.0 |

The top self-time growth (µs per request): `__syscall_cancel_arch` +60.4, `[vdso]` +43.5, `do_syscall_64` +20.0,
`__internal_syscall_cancel` +6.1, `fdget` +6.1, `EPoll.wait` +5.0, `SelectorImpl.processDeregisterQueue` +4.0,
`EPollSelectorImpl.processUpdateQueue` +3.8, `_raw_spin_unlock_irqrestore` +3.5, `os::javaTimeNanos` +3.3.
The top self-time shrinkers are `CompletableFuture.uniComposeStage` −1.8, `VarHandle ... compareAndSet` −1.3,
`CompletableFuture.unipush` −0.6, `CompletableFuture.newIncompleteFuture` −0.6 and `SocketConnectionBase.prepareCommand` −0.6:
that is Hibernate Reactive 4.5 against 3.4 and the SQL client, it is small and in the other direction.

How many calls: async-profiler's Java method profiling with interval 1 counts the calls (`profile/count_calls.sh`):

| Calls per request | `v4` | `vertx5` |
|---|---|---|
| `EPollSelectorImpl.doSelect` (one `epoll_wait`) | 9.9 | **79.1** |
| `SocketChannelImpl.read` | 10.5 | 10.5 |
| `SocketChannelImpl.write` | 9.5 | 9.5 |

The reads and writes are identical (same queries, same bytes, no extra I/O).
Only the selector is polled eight times as often.
A loop handles about 1000 requests/s, so on Vert.x 5 it polls the selector every 13 µs.

Things looked for and **not** found in the difference:
context dispatch (`ContextBase.emit` +3 µs, in line with the I/O phase),
task queue (`runAllTasks` +11 µs, which is the tasks a polling loop sees; the task work itself is the same),
promise and future machinery (`CompletableFuture` self time is smaller on Vert.x 5, `FutureBase`/`PromiseImpl` only change name),
buffer handling and the codec (`PgDecoder`, `PgEncoder`, Netty buffers: ±1 µs),
`Thread.yield` and park (not in the profile), locks, logging, tracing and metrics hooks.
Allocation is +6% (60.9 KB against 57.2 KB per request, section 9) and doesn't matter.

### Hibernate Reactive 3.4 against 4.5

To separate it from the Vert.x change, look at the I/O phase (reads, decode, callbacks, all of Hibernate Reactive
and the SQL client) on its own: with the cache on and no stealing it is 59.0 µs on `v4` and 57.8 µs on `vertx5`.
With the Netty patch the whole request costs 58 µs (`v4`) and 60 µs (`vertx5`).
The difference caused by Hibernate Reactive 3.4 against 4.5 is therefore less than 5%.
The `org.hibernate.reactive` self time is 5.9 µs (`v4`) and 3.8 µs (`vertx5`) per request.
The Vert.x-only reproducer (section 7) has no Hibernate at all and shows the same gap.

### Stealing

With the patch, `shared` still costs more on Vert.x 5 than on 4.5 (150 µs against 133 µs; 97 µs against 82 µs with the cache).
The Vert.x-only reproducer shows it too (131 µs against 112 µs). Profiles of the reproducer (patched Vert.x 5 against 4.5,
`shared`, 74.5 µs against 111.7 µs in the profile) put +30 µs in the selector poll and +3.7 µs in the loop,
and nothing in the I/O phase.
So `NioIoHandler` is still busier than `NioEventLoop` when other threads submit tasks (steals do that).
The native epoll transport doesn't have this (107 µs against 112 µs for `v4`). This is a second, smaller finding
and it needs its own look; it is not what the issue below is about.

## 6. The cause, in the Netty code

`NioIoHandler.select(...)`, Netty 4.2.19, runs when the event loop has nothing to do.
Vert.x timers are scheduled tasks of the event loop, so there is always a deadline:

```java
long millisBeforeDeadline = millisBeforeDeadline(selectDeadLineNanos, currentTimeNanos);
if (millisBeforeDeadline <= 0) {
    if (selectCnt == 0) {
        selector.selectNow();      // does not sleep
        selectCnt = 1;
    }
    break;                         // back to SingleThreadIoEventLoop.run(): runIo, runAllTasks, runIo, ...
}
timeoutMillis = millisBeforeDeadline;
...
int selectedKeys = selector.select(timeoutMillis);

private static long millisBeforeDeadline(long selectDeadLineNanos, long currentTimeNanos) {
    ...
    // Add 500_000 to round up to the nearest millisecond.
    return (nanosBeforeDeadline + 500_000L) / 1_000_000L;     // this rounds to the nearest
}
```

When the deadline is less than 0.5 ms away this returns 0, `select` does a `selectNow()` and returns,
the task that is not due yet doesn't run, and the loop calls `select` again.
It spins until the deadline: up to 0.5 ms of CPU before every timer, and every time the loop wakes up
for an I/O event with a timer pending (the comment says "round up", the code rounds to the nearest).

Netty 4.1.139 (`NioEventLoop.select(long)`) rounds up:

```java
// Timeout will only be 0 if deadline is within 5 microsecs
long timeoutMillis = deadlineToDelayNanos(deadlineNanos + 995000L) / 1000000L;
return timeoutMillis <= 0 ? selector.selectNow() : selector.select(timeoutMillis);
```

so it sleeps in `epoll_wait` until the deadline (a timer can run up to 1 ms late, and the loop doesn't use CPU while it waits).

It needs the loop to wake up between timers, so an idle loop doesn't show it (an idle Netty 4.2 loop with one
1 ms timer uses 0.3% of a core, like 4.1). A loop with I/O does, in proportion to how often it wakes up
(section 7). The `LoadVerticle` of the benchmark and any application with timers (idle timeouts, pool timeouts,
`setTimer`, `setPeriodic`) are the same.

## 7. Reproducers

### Vert.x only: `vertx-repro` (`VertxCpuRepro`)

A verticle on each of 4 event loops starts requests with Poisson arrivals from a Vert.x timer (like `LoadVerticle`).
A request takes a connection from a Vert.x PostgreSQL pool, runs 5 prepared queries
(`select id, random_number from repro_world where id = $1`) on it one after the other, and gives it back.
Same options as above (4000 requests/s, 4 loops, pool of 4, 10 s warm-up, 20 s measured), 2 runs each:

| Case | Vert.x 4.5.35 (Netty 4.1.139) | Vert.x 5.1.10 (Netty 4.2.19) | Vert.x 5.2.2 (Netty 4.2.19) | 5.1.10 + Netty patch | 5.1.10, native epoll |
|---|---|---|---|---|---|
| `perEventLoop`, cache off | 82 µs | **261 µs** | 264 µs | 86 µs | 83 µs |
| `perEventLoop`, cache on | 41 µs | **87 µs** | 88 µs | 43 µs | 41 µs |
| `shared`, cache off | 112 µs | **358 µs** | 359 µs | 131 µs | 107 µs |
| `shared`, cache on | 66 µs | **179 µs** | 182 µs | 78 µs | 64 µs |
| `perEventLoop`, cache off, requests started by a thread, no Vert.x timer | 82 µs | 90 µs | | 89 µs | |

What the numbers show:

- The gap reproduces without Hibernate Reactive, and is as large: 3.2 times (cache off), 2.1 times (cache on).
  It is the same on Vert.x 5.2.2, the latest 5.x.
- It disappears when no Vert.x timer exists: a plain thread hands the requests to the event loops with `runOnContext`
  and the CPU is 82 against 90 µs. With a timer it is 82 against 261 µs.
- The profile of the reproducer shows the same phases as the benchmark:
  selector poll 24.0 → 174.3 µs, loop 1.0 → 35.2, tasks 4.0 → 12.2, I/O 31.1 → 39.3.
- The native epoll transport (`nativeTransport=true` with `netty-transport-native-epoll` on the class path) is a workaround.

### Netty only: `profile/NettyTimerSpin.java`

One `NioEventLoopGroup` thread, a scheduled task that runs every 1 ms, and a second thread that submits empty tasks
at random times (Poisson, like I/O events). The CPU of the event loop thread over 5 s:

| Submitted tasks per second | Netty 4.1.139 | Netty 4.2.19 |
|---|---|---|
| 0 | 0.3% of a core | 0.3% |
| 1000 | 0.5% | **10.5%** |
| 4000 | 1.1% | **28.2%** |
| 10000 | 2.1% | **41.0%** |

Netty 4.2 ran 4644–5010 timer tasks in 5 s and Netty 4.1 3870–4560: the 4.1 timers are late.

## 8. The fix that was tested

`profile/netty-4.2-round-up.patch` is the whole change: `NioIoHandler.millisBeforeDeadline` adds `995_000L` like Netty 4.1
instead of `500_000L`. I compiled that class from the Netty 4.2.19 sources and put it in front of the Netty jar
(no other change). Section 3 and 7 show the result:

- Benchmark, `perEventLoop`, cache off: 297 → 103 µs per request (`v4`: 102). Sleeps per request: 7.4 → 10.4 (`v4`: 10.4).
- Vert.x-only reproducer: 261 → 86 µs (`v4`: 82).
- Cost: timers can fire up to 1 ms late, like with Netty 4.1 and Vert.x 4.5. The median latency of the benchmark goes up by 0.3 ms
  (1449 µs → 1727 µs, `v4`: 1749 µs), which is the timer delay.

The real fix, with tests, is a branch of Netty 4.2 (`~/netty`, `fix-nio-select-timeout-rounding`), see `NETTY_SELECT_ROUNDING.md`.
The `500_000L` was not a deliberate precision change: Netty 4.1 had it too until #9799 (2019) replaced it with `995000L`,
and the 4.2 `NioIoHandler` (#13991, 2024) was written with the old code.

Workarounds for applications:

- Use the native transport (`setPreferNativeTransport(true)` and the `netty-transport-native-epoll` jar): 83 µs in the reproducer.
- Don't use many timers on event loops that also do I/O (not realistic).

## 9. Allocation

One `alloc` recording per stack (`--alloc 512k`), `perEventLoop`, cache off, per request:

| | `v4` | `vertx5` |
|---|---|---|
| Allocated per request | 57.2 KB | 60.9 KB (+6%) |
| `java.nio.DirectByteBuffer` | 0 | +2.0 KB |
| `CompletableFuture` | 12.8 KB | 13.2 KB |

Vert.x 5 allocates a little more (a `DirectByteBuffer` per read, the 5.x command objects) and that is not the CPU gap:
2 KB per request is not 190 µs. The event loop spin itself doesn't allocate.

## 10. Caveats

- The `cpu-clock` profile of an event loop that sleeps a lot misses some of the CPU:
  in the Vert.x-only reproducer the profile of `v4` adds up to 60 µs per request while `ThreadMXBean` says 82–91 µs.
  The `vertx5` profiles add up to what the JVM reports. So the profiles understate `v4`, which makes the gap look bigger in the profile
  than it is (4.3 times in the profile against 3.2 times measured). Only use the section 3 and 7 numbers for the size of the gap,
  and the profiles for where it is.
- One machine, one kernel, one CPU pinning. The container has a seccomp filter, so syscalls are a bit more expensive here
  (`seccomp_run_filters` is 1% of the time). On bare metal the spin costs less per iteration, but 8 times more `epoll_wait` is 8 times more.
- 4000 requests/s on 4 loops is 17% busy per loop. A busier loop spins less (it is less often idle with a timer pending)
  and a quieter one is not affected as much. I didn't measure other rates here.
- The numbers in this document use a patched copy of `NioIoHandler` on the class path. The real fix was built later
  and measured in `NETTY_SELECT_ROUNDING.md`.
- The residual `shared` difference (section 5) is not explained.
- The benchmark latencies include the delay of the timers that start the requests, so the latency comparison has the 0.3 ms
  rounding difference in it.
- mcp-jafar was used for cross-checks, not for the tables. Its `jfr_query` language rejected my `groupBy` on stack frames
  (the `[0]` index in a predicate), so the per frame aggregation is done on collapsed stacks.

## 11. The Vert.x issue

The root cause is in Netty, so the issue is a Netty issue. The drafts (Netty issue, Netty pull request and a short note for Vert.x)
and the status of the fix are in `NETTY_SELECT_ROUNDING.md`.

## 12. Files and how to run it

In git (`profile/` and `vertx-repro/`):

| File | What it is |
|---|---|
| `profile/profile_run.sh` | One benchmark run with async-profiler attached for the measured window only |
| `profile/run_matrix.sh` | Both stacks, both pools, cache off and on, cpu-clock and unprofiled runs, then `alloc` |
| `profile/convert.sh` | JFR to collapsed stacks, flame graphs and differential flame graphs |
| `profile/analyze.py` | Per request normalisation, phases, packages, differential tables |
| `profile/summarize.py` | Averages the `RESULT` lines of the runs |
| `profile/count_calls.sh` | Counts the calls of `doSelect`, `read` and `write` with method profiling |
| `profile/run_repro.sh`, `profile/run_repro_matrix.sh` | The Vert.x-only reproducer on each Vert.x version |
| `profile/NettyTimerSpin.java` | The Netty-only reproducer |
| `profile/netty-4.2-round-up.patch` | The one line Netty change |
| `vertx-repro/` | The Vert.x-only reproducer (`VertxCpuRepro`) |

Raw files (not in git), in `/tmp/profile`:

| Path | Content |
|---|---|
| `/tmp/profile/out/cpu-<stack>-<pool>-cache<c>-r<n>.jfr` | async-profiler `cpu-clock` recordings of the benchmark (16, 200 µs interval) |
| `/tmp/profile/out/alloc-<stack>-<pool>-cachefalse-r1.jfr` | `alloc` recordings (4) |
| `/tmp/profile/out/none-*.log`, `repro-*.log` | Unprofiled runs (`RESULT` line at the end) |
| `/tmp/profile/out/reprocpu-*.jfr` | `cpu-clock` recordings of the Vert.x-only reproducer |
| `/tmp/profile/out/calls-*.jfr` | Method profiles that count calls |
| `/tmp/profile/col/*.collapsed` | Collapsed stacks, split by thread |
| `/tmp/profile/col/cpu-*.html`, `alloc-*.html` | Flame graphs |
| `/tmp/profile/col/diff-<pool>-cache<c>.html` | Differential flame graphs, Vert.x 5 against Vert.x 4.5 (red is more in Vert.x 5) |
| `/tmp/profile/analysis/*.md` | Full `analyze.py` output (top self and total time, differences) and the mcp-jafar output |
| `/tmp/profile/ap` | async-profiler 4.6 |
| `/tmp/profile/patch/nio-patched.jar` | The patched `NioIoHandler` class |

To repeat it:

```
gradle :hibernate-orm-benchmark-reactive-openloop:installDist                 # copy target/install/* to /tmp/profile/install-v4
gradle :hibernate-orm-benchmark-reactive-openloop:installDist -Pstack=vertx5  # copy it to /tmp/profile/install-vertx5
./start_pg.sh
profile/run_matrix.sh && profile/convert.sh
python3 profile/analyze.py "v4=/tmp/profile/col/cpu-v4-perEventLoop-cachefalse-r*.collapsed" \
    "v5=/tmp/profile/col/cpu-vertx5-perEventLoop-cachefalse-r*.collapsed"
```

# Netty 4.2: the NIO event loop busy-polls the selector before timers

`VERTX5_CPU_PROFILE.md` found that Vert.x 5 uses 2–3 times the event loop CPU of Vert.x 4.5.
The cause is in Netty's `NioIoHandler`. This document has the status of the fix, the measurements with the real fix,
and the drafts of the Netty issue, the Netty pull request and a note for Vert.x. **Nothing was posted or pushed.**

## 1. Status

| Step | State |
|---|---|
| Cause found in the profiles (`VERTX5_CPU_PROFILE.md`) | done |
| Cause confirmed in the sources (4.1 vs 4.2) and by a patched class | done |
| History of the line checked (git log, GitHub API) | done: it is a leftover of the old 4.1 code, not a deliberate change |
| Other IoHandlers checked (epoll, kqueue, io_uring) | done: not affected. Netty 5.0 has the same bug |
| Fix in `~/netty`, branch `fix-nio-select-timeout-rounding`, 2 local commits | done |
| Tests: rounding and no busy spin, both fail without the fix | done |
| `transport` module tests, checkstyle | done: 423 run, 0 failures, 9 skipped |
| Effect measured with the built Netty (Netty-only, Vert.x-only, Hibernate Reactive) | done (section 4) |
| Netty issue | **draft only, not filed** (section 6) |
| Netty pull request | **not opened**, draft only (section 7) |
| Vert.x issue/discussion | **draft only, not posted** (section 8) |
| ICLA | **not signed** (section 3) |
| Not done | the `testsuite` module (socket/transport suites), the native modules (epoll, kqueue, io_uring: no native toolchain was used), a build on JDK 8–21, the 5.0 branch |

## 2. The fix, what blame showed

`~/netty`, branch `fix-nio-select-timeout-rounding` from `4.2` (`6deec69` at the time):

```
268f295 Test the rounding of the NIO select timeout
656e619 Round up the NIO select timeout again to not busy spin
```

The change is in `NioIoHandler.millisBeforeDeadline(...)`: `+ 995_000L` instead of `+ 500_000L`, the overflow guard uses
the same bias, and the method is package private for the test. The timeout is 0 only when the deadline is less than 5 µs away,
like in 4.1. The Javadoc says so.

History of the `500_000L`:

- Netty 4.1 had `(selectDeadLineNanos - currentTimeNanos + 500000L) / 1000000L` in `NioEventLoop.select()` for years
  (e.g. in `a22d4ba8`, 2019-08).
- **#9799 "Clean up NioEventLoop" (`e208e96f`, 2019-11)** rewrote it as
  `deadlineToDelayNanos(deadlineNanos + 995000L) / 1000000L` with the comment
  "Timeout will only be 0 if deadline is within 5 microsecs". The commit message doesn't mention the rounding;
  it mirrors what `EpollEventLoop` did.
- **#13991 (`596a1f0b`, 2024-04)** introduced `NioIoHandler` and removed the 4.1 style `NioEventLoop.select(long)` from 4.2.
  The new `select(IoHandlerContext, boolean)` brought back the old pre-#9799 loop, with `+ 500000L`.
  Netty 4.2.0.Final has it (line 619). The current code has it in `millisBeforeDeadline(...)` with an overflow guard and the comment
  "Add 500_000 to round up to the nearest millisecond" (I didn't look up which commit moved it there).
- So the rounding was not changed on purpose for precision. The comment says "round up" and the code rounds to the nearest.
  The 4.2 branch is affected from 4.2.0 (all `NioIoHandler` users: `NioEventLoopGroup`, `MultiThreadIoEventLoopGroup`
  with `NioIoHandler.newFactory()`). I measured 4.2.19 only.
- I found no issue in `netty/netty` about it (searched for the method, "selectNow", "busy", "CPU" with 4.2).

Other IoHandlers (4.2 branch):

| Handler | Timeout | Affected |
|---|---|---|
| `NioIoHandler` | `Selector.select(ms)`, millisecond resolution | **yes** |
| `EpollIoHandler` | a `timerfd` with a nanosecond deadline, `epollWait(..., delaySeconds, delayNanos)` | no |
| `KQueueIoHandler` | `keventWait(seconds, nanos)` | no |
| `IoUringIoHandler` | `addTimeout` with a `timespec` in nanoseconds | no |
| `LocalIoHandler` | no selector | no |

Netty `5.0` (there is no `main` branch; the default branch is `4.2`) has the same `+ 500_000L` in `NioIoHandler`.
The 4.2 pull request is ported to 5.0 by the `autoport-42` workflow.

The fix keeps the 4.1 behaviour: the loop sleeps in `select(ms)` with the timeout rounded up, so a timer can run up to about
1 ms late. A shorter sleep isn't possible with `Selector.select(long)`; the native transports have no such limit.

## 3. What Netty asks from the contributor

- From the developer guide (https://netty.io/wiki/developer-guide.html): sign the Individual Contributor License Agreement
  (https://netty.io/s/icla), or the employer signs the CCLA, **unless the contribution is trivial** such as a single line change.
  This fix is a one line change with a test; whether it counts as trivial is the maintainer's call. I signed nothing.
- No `Signed-off-by` is required by the guide. The commit message form is the one with Motivation / Modification(s) / Result.
  The PR template has the same three headings plus `Fixes #<issue>`.
- Commits of a PR are squashed into a small number of commits. The branch has 2 (fix, then test) as asked.
- Rebase on the target branch (`4.2`).
- Checkstyle runs in the build (`check-style`) and it caught a 121 character line in my first version (limit 120).
- The commit messages of this branch end with `Assisted-by: Claude Opus 5.5 <noreply@anthropic.com>`, as asked.
  Check that Netty is fine with that trailer before opening the PR.

## 4. Verification

### Tests

- `NioIoHandlerTest` (existing class, 2 new tests + 1 behaviour test):
  - `testMillisBeforeDeadlineRoundsUp`: 0 for a deadline already reached or within 4 999 ns; 1 for 5 µs to 1.004999 ms
    (including 100 µs, 499 999 ns, 500 000 ns, 999 999 ns); 2 from 1.005 ms; 1000 for one second.
  - `testMillisBeforeDeadlineDoesNotOverflow`: `Long.MAX_VALUE - 1`, `- 995_000`, `- 995_001`, `- 1_000_000`
    return the cap; `Long.MAX_VALUE - 10_000_000` is positive.
  - `testNoBusySpinIfDeadlineIsLessThanHalfAMillisecondAway`: drives a `NioIoHandler` with an `IoHandlerContext` whose next
    scheduled task is 300 µs away and calls `run(...)` like an event loop does until the deadline. It must return at most
    3 times. It is deterministic (no timing assertion on the happy path: the fixed code sleeps in `select(1)`).
- **Without the fix** the rounding test fails (`expected: <1> but was: <0>`) and the behaviour test fails
  (`run(...) returned 205 times before the deadline`). With the fix 3 runs of the class passed (4 tests).
- Module `transport`, `./mvnw -pl transport test` (JDK 25.0.4, Linux): **423 tests, 0 failures, 0 errors, 9 skipped**, BUILD SUCCESS.
  This includes `NioEventLoopTest` (13), `NioIoHandlerEventCountTest`, `NioSocketChannelTest` (8), `MultiThreadIoEventLoopGroupTest`.
  Checkstyle (`check-style`) ran on `transport` and its dependencies.
  `./mvnw -pl transport -am install -DskipTests` built `common`, `buffer`, `resolver`, `transport` and installed
  `4.2.20.Final-SNAPSHOT` in `~/.m2`.
- Not run: `testsuite` (socket tests across transports), native modules, other JDKs. Each commit builds: the first one was built
  with checkstyle (`install -DskipTests`) and the whole branch runs the transport tests.

### Netty only (`profile/NettyTimerSpin.java`)

One `NioEventLoopGroup` thread, a task scheduled again every 1 ms, and another thread submitting empty tasks at random times.
CPU of the event loop thread over 5 s, and how late the 1 ms timer runs (µs). One run each, JVM pinned to 8 cores:

| Tasks/s submitted | Netty 4.1.139 | Netty 4.2.19 (stock) | Netty 4.2.20-SNAPSHOT (the fix) |
|---|---|---|---|
| 0 | 0.3% | 0.3% | 0.3% |
| 1000 | 0.5% | **10.5%** | 0.6% |
| 4000 | 1.1% | **27.9%** | 1.7% |
| 10000 | 2.0% | **40.9%** | 3.2% |

Timer lateness at 4000 tasks/s: 4.1 mean 226 µs, p50 161, p99 900, max 1039; 4.2.19 mean **25**, p50 0, p99 423;
the fix mean 226, p50 163, p99 911, max 1040 (same as 4.1).
Calls of `EPollSelectorImpl.doSelect` (method profiling, about 6 s): 4.1: 20 509, 4.2.19: **845 369**, the fix: 40 242
(41 times more in stock 4.2, 2 times 4.1 with the fix).

The fix still uses about 1.5 times the CPU of 4.1 (1.7% against 1.1%): the 4.2 `NioIoHandler` does a little more per iteration.
That is a different, small thing.

### Vert.x only (`vertx-repro`, 4000 requests/s, 4 event loops, pool of 4, 2 runs each)

Event loop CPU per request, Vert.x 5.1.10 with stock Netty 4.2.19 and with the fix (`4.2.20.Final-SNAPSHOT` for `netty-transport`,
`-common`, `-buffer`, `-resolver`; other Netty jars 4.2.19), and Vert.x 4.5.35 (Netty 4.1.139):

| Case | Vert.x 4.5.35 | Vert.x 5.1.10, stock | Vert.x 5.1.10, the fix |
|---|---|---|---|
| `perEventLoop`, cache off | 96 µs (79–112) | **260 µs** (253–266) | 87 µs (86–87) |
| `perEventLoop`, cache on | 40 µs (40–41) | **89 µs** (87–92) | 44 µs (42–46) |
| `shared`, cache off | 124 µs (119–128) | **368 µs** (365–372) | 135 µs (133–137) |
| `shared`, cache on | 70 µs (65–74) | **180 µs** (178–183) | 79 µs (77–80) |

(The Vert.x 4.5 `perEventLoop` cache off runs are noisier than the 82 µs of the earlier session: 79 and 112 µs.)
The fix brings Vert.x 5 to the Vert.x 4.5 level. With `shared` (connections stolen between loops) it stays about 10 µs above;
that was seen before with the patched class and is not explained (the native transport doesn't have it).

### Hibernate Reactive benchmark (`perEventLoop`, cache off, 4000 requests/s)

| Stack | Event loop CPU per request | Sleeps per request | p50 | p99 |
|---|---|---|---|---|
| Hibernate Reactive 3.4.6 + Vert.x 4.5.35 | 102 µs (98–105) | 10.4 | 1746 µs | 3.9 ms |
| Hibernate Reactive 4.5.11 + Vert.x 5.1.10, stock Netty | **289 µs** (288–290) | 7.4 | 1438 µs | 3.5 ms |
| same, Netty with the fix | **107 µs** (106–109, 4 runs) | 10.3 | 1737–1763 µs | 3.9–4.3 ms (23 ms once) |

One more run with the fix had 131 µs and a 2 s stall (p90 1.35 s, 0.38 preemptions per request against 0.03–0.08 in
the others, 2294 requests in flight): something took CPUs from the pinned JVM during that run. The next 4 runs were normal.
I couldn't tie it to anything, and I couldn't reproduce it, but it is one run in five, so look at it if it comes back.

### The latency side effect

With the fix, timers can run later than with stock 4.2: up to 1 ms, the same as in Netty 4.1 and Vert.x 4.5.

- Netty only: mean lateness 25 → 226 µs, p99 423 → 911 µs, max about 1 ms (4000 tasks/s), identical to 4.1.
- Benchmark: the median latency goes from 1438 µs (stock 4.2) to about 1750 µs (the fix, and Vert.x 4.5: 1746 µs). That is the
  start delay of the 1 ms timers of the load generator, which is part of the measured latency.
- It applies to timers only when the loop wakes up for another reason with a timer less than 1 ms away; a timer that is the only
  thing the loop waits for is rounded to the next millisecond in both versions.
- If that precision matters to someone, the stock behaviour costs 8–40 times more `epoll_wait` calls. A cheaper way to get
  both would be a bounded spin (for a few microseconds) or a different timeout, but `Selector.select(long)` has a
  millisecond resolution. The epoll, kqueue and io_uring transports have precise timers and don't spin.

## 5. Where things are

| What | Path |
|---|---|
| Netty clone and fix branch | `~/netty`, branch `fix-nio-select-timeout-rounding` (`git log origin/4.2..HEAD`) |
| Test log of the `transport` module | `/tmp/netty-transport-test.log`, build log `/tmp/netty-build1.log` |
| Built Netty snapshot | `~/.m2/repository/io/netty/netty-{common,buffer,resolver,transport}/4.2.20.Final-SNAPSHOT/` |
| Class path with the fix, for `NettyTimerSpin` | `/tmp/profile/netty-4.2p/` (stock 4.2.19: `netty-4.2/`, 4.1.139: `netty-4.1/`) |
| Vert.x only install with the fix / stock / Vert.x 4.5 | `/tmp/profile/repro-vertx5s`, `repro-vertx5`, `repro-v4` |
| Benchmark install with the fix / stock / Vert.x 4.5 | `/tmp/profile/install-vertx5s`, `install-vertx5`, `install-v4` |
| Raw logs of the final runs | `/tmp/profile/out2/{repro,hr}-*.log` |
| Netty only results | `/tmp/profile/netty-spin2.txt`, select counts `/tmp/profile/out/spin-*.collapsed` |
| Everything from the earlier profiling | see `VERTX5_CPU_PROFILE.md` section 12 |
| Fix as a patch (the earlier one line experiment) | `profile/netty-4.2-round-up.patch` |

Re-run:

```
# Netty only (class path: the jars of the version to test; the fix needs ~/netty built with: ./mvnw -pl transport -am install -DskipTests)
java -cp 'netty-4.2/*'  profile/NettyTimerSpin.java 1 5 4000
java -cp 'netty-4.2p/*' profile/NettyTimerSpin.java 1 5 4000     # netty-4.2 with the 4 snapshot jars instead of 4.2.19
java -cp 'netty-4.1/*'  profile/TimerSpin.java                    # the short version of the issue

# Vert.x only and the benchmark (PostgreSQL: ./start_pg.sh)
gradle :hibernate-orm-benchmark-vertx-repro:installDist -Pstack=vertx5     # then replace the 4 netty jars (and the names in bin/*) by the snapshots
OUT=/tmp/profile/out2 profile/run_repro.sh vertx5s repro-vertx5s-perEventLoop-cachefalse-r1 rate=4000 poolType=perEventLoop
OUT=/tmp/profile/out2 EVENT=none profile/profile_run.sh vertx5s hr-vertx5s-perEventLoop-cachefalse-r1 rate=4000 poolType=perEventLoop poolSize=4
python3 profile/summarize.py /tmp/profile/out2 repro- hr-

# The Netty tests
cd ~/netty && ./mvnw -pl transport test -Dtest=NioIoHandlerTest -Dsurefire.failIfNoSpecifiedTests=false
```

## 6. Draft: Netty issue

**Title:** NioIoHandler: event loop busy-polls with `selectNow()` when a scheduled task is less than 0.5 ms away (4.2 regression vs 4.1)

**Netty version:** 4.2.19.Final (also 4.2.0.Final by the code; the `5.0` branch has the same code). Not affected: 4.1.x.

**JDK / OS:** OpenJDK 25.0.4 (Red Hat), Linux 7.2 x86_64 (also a container). NIO transport.

**Expected behavior**

An event loop that waits for a scheduled task sleeps in `Selector.select(timeout)` until the task is due, as in 4.1.

**Actual behavior**

When the next scheduled task is less than 0.5 ms away, `NioIoHandler.select(...)` calls `selector.selectNow()` and returns.
The task is not due yet, so `SingleThreadIoEventLoop.run()` loops (`runIo`, `runAllTasks`, `runIo`, ...) and `select(...)` does the same again,
until the deadline. The event loop polls the selector in a loop for up to 0.5 ms before every timer, every time it woke up
for another reason (I/O, a submitted task) while a timer is pending.

In an application with a 1 ms timer (Vert.x timers are Netty scheduled tasks), 4 event loops and 4000 requests/s,
the event loops used about 3 times the CPU of the same application on Netty 4.1 (289 µs against 102 µs of event loop CPU per request)
with 8 times as many `epoll_wait` calls (79 against 10 per request), while reads and writes were identical.
`async-profiler` (`cpu-clock`): `NioIoHandler.select` -> `SelectorImpl.selectNow` -> `EPoll.wait` is 50% of the event loop CPU,
plus `clock_gettime` (`SystemTicker.nanoTime`, `reportActiveIoTime`) and the loop around it: 91% of the difference to 4.1.

**Cause**

4.2 `NioIoHandler`:

```java
private static long millisBeforeDeadline(long selectDeadLineNanos, long currentTimeNanos) {
    ...
    // Add 500_000 to round up to the nearest millisecond.
    return (nanosBeforeDeadline + 500_000L) / 1_000_000L;     // rounds to the nearest millisecond
}
...
long millisBeforeDeadline = millisBeforeDeadline(selectDeadLineNanos, currentTimeNanos);
if (millisBeforeDeadline <= 0) {
    if (selectCnt == 0) {
        selector.selectNow();
        selectCnt = 1;
    }
    break;
}
```

4.1 `NioEventLoop` (since #9799):

```java
// Timeout will only be 0 if deadline is within 5 microsecs
long timeoutMillis = deadlineToDelayNanos(deadlineNanos + 995000L) / 1000000L;
return timeoutMillis <= 0 ? selector.selectNow() : selector.select(timeoutMillis);
```

The 4.2 code is the pre-#9799 4.1 code, brought back with `NioIoHandler` (#13991). The comment says "round up".

**Reproducer** (no Vert.x; run with the jars of Netty 4.1 or 4.2 on the class path: `java -cp 'netty/*' TimerSpin.java`)

```java
import io.netty.channel.nio.NioEventLoopGroup;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TimerSpin {
    public static void main(String[] args) throws Exception {
        var loop = new NioEventLoopGroup(1).next();
        long[] tid = new long[1];
        Runnable[] timer = new Runnable[1];
        // a timer every 1 ms, like io.vertx.core.Vertx#setTimer(1, ...)
        timer[0] = () -> { tid[0] = Thread.currentThread().threadId(); loop.schedule(timer[0], 1, TimeUnit.MILLISECONDS); };
        loop.schedule(timer[0], 1, TimeUnit.MILLISECONDS);
        // other threads wake the loop up now and then, like I/O events: 4000 tasks/s
        Thread t = new Thread(() -> {
            for (;;) {
                LockSupport.parkNanos((long) (-Math.log(1 - Math.random()) * 250_000));
                loop.execute(() -> { });
            }
        });
        t.setDaemon(true);
        t.start();
        TimeUnit.SECONDS.sleep(1);
        var mx = ManagementFactory.getThreadMXBean();
        long before = mx.getThreadCpuTime(tid[0]);
        TimeUnit.SECONDS.sleep(5);
        long ms = (mx.getThreadCpuTime(tid[0]) - before) / 1_000_000;
        System.out.println(io.netty.util.Version.identify().get("netty-transport").artifactVersion()
                + ": event loop CPU " + ms / 50.0 + "% of a core");
        System.exit(0);
    }
}
```

Event loop thread CPU, 5 s, 1 ms timer, other tasks submitted at random times (a longer version prints the timer lateness too):

| Tasks submitted per second | 4.1.139 | 4.2.19 | 4.2.20-SNAPSHOT with the fix |
|---|---|---|---|
| 0 (idle loop) | 0.3% | 0.3% | 0.3% |
| 1000 | 0.5% | 10.5% | 0.6% |
| 4000 | 1.1% | 27.9% | 1.7% |
| 10000 | 2.0% | 40.9% | 3.2% |

`EPollSelectorImpl.doSelect` calls in about 6 s at 4000 tasks/s: 20 509 (4.1), 845 369 (4.2.19), 40 242 (the fix).
An idle loop is not affected; the CPU grows with how often the loop wakes up with a timer pending.

The native epoll transport is **not** affected (`EpollIoHandler` uses a `timerfd` with a nanosecond deadline; same for kqueue and io_uring):
Vert.x 5.1.10 on `netty-transport-native-epoll` used 83 µs per request, against 261 µs with NIO and 82 µs with Vert.x 4.5.

**Proposed fix**

Round up like 4.1: `return (nanosBeforeDeadline + 995_000L) / 1_000_000L;` and use the same bias in the overflow guard.
The timeout is 0 only if the deadline is within 5 µs. With it: 4000 tasks/s: 1.7% instead of 27.9%; the Vert.x application above:
107 µs instead of 289 µs per request (Vert.x 4.5: 102 µs).

**Trade-off:** the loop sleeps until the next whole millisecond, so a timer can run up to 1 ms late when the loop woke up for something
else with the timer less than 1 ms away: at 4000 tasks/s the mean lateness of a 1 ms timer goes from 25 µs to 226 µs
(p99 423 µs to 911 µs), like in 4.1. A selector-based loop can't do better (`Selector.select(long)` has a millisecond
resolution); if the precision matters, a bounded spin could be an option. Is the precision of 4.2 wanted? The comment suggests rounding up was the intent.

Pull request: (link when opened).

## 7. Draft: Netty pull request

**Title:** Round up the NIO select timeout again to not busy spin

(Branch `4.2`. Two commits: the fix, then the test.)

```
Motivation:

NioIoHandler.millisBeforeDeadline(...) adds 500_000 ns before dividing by 1_000_000, which rounds to the nearest millisecond,
while the comment says it rounds up. If the next scheduled task is less than 0.5 ms away the timeout is 0, so select(...) calls
selectNow() and returns. The task is not due yet, so the event loop runs another iteration and does the same again until the
deadline is reached. An event loop that has I/O and a pending timer therefore polls the selector in a loop: an application with
a timer that fires every ms, 4 event loops and 4000 requests/s used about 3 times the CPU of 4.1, with 8 times as many
epoll_wait calls.

NioEventLoop in 4.1 rounds up (deadline + 995000) since #9799, so the timeout is only 0 if the deadline is within 5 microseconds.
The 4.2 NioIoHandler was introduced (#13991) with the older rounding.

Modification:

Add 995_000 instead of 500_000 and adjust the overflow guard to the new bias. Make the method package private so it can be tested.
Test millisBeforeDeadline(...), including the values near Long.MAX_VALUE, and test that run(...) does not return many times while
a scheduled task is 300 microseconds away (it did 205 times before the fix).

Result:

Fixes #<issue>.

The event loop sleeps in Selector.select(...) until the deadline instead of spinning. Timers can fire up to 1 ms later than before,
like in 4.1. The epoll, kqueue and io_uring transports use nanosecond timeouts and are not affected.
With a timer every 1 ms and 4000 tasks/s: event loop CPU 27.9% -> 1.7% of a core (4.1: 1.1%).
```

Notes for the PR: the 5.0 branch has the same code (the autoport workflow ports 4.2 PRs); ICLA question (section 3);
`Assisted-by` trailer; the module `transport` tests pass (423, 9 skipped), `testsuite` not run.

## 8. Draft: note for Vert.x (issue or discussion)

**Title:** Vert.x 5 on NIO: event loops busy-poll the selector when timers are pending (Netty 4.2), about 3x CPU per request

We saw 2.5–3 times the event loop CPU per request on Vert.x 5.1.10 (Netty 4.2.19) compared to Vert.x 4.5.35 (Netty 4.1.139),
in a Hibernate Reactive + Vert.x PostgreSQL client benchmark (4000 requests/s, 4 event loops, 5 queries per request:
289 µs against 102 µs). A Vert.x-only reproducer without Hibernate (a `Pool`, 5 prepared queries per request, one `setTimer` driven
request generator per event loop) shows 260 µs against 87–96 µs, and the same on Vert.x 5.2.2.
Profiles (async-profiler, `cpu-clock`) show the whole difference in `NioIoHandler.select` -> `selectNow` -> `epoll_wait`:
79 `epoll_wait` per request instead of 10, with the same number of reads and writes.

The cause is in Netty 4.2: `NioIoHandler` rounds the select timeout to the nearest millisecond, so when a scheduled task
(any Vert.x timer) is less than 0.5 ms away it polls with `selectNow()` in a loop instead of sleeping. Netty 4.1 rounds up.
Netty issue: <link, once filed>. Netty fix (not merged yet): <link to the PR once opened>.
With the fix, Vert.x 5.1.10 uses the Vert.x 4.5 CPU (87 µs and 107 µs per request in our two setups).

What users see: higher CPU per request on NIO event loops that have a timer pending and I/O at the same time
(idle timeouts, pool timeouts, `setTimer`, `setPeriodic`), growing with the request rate.
Applications that don't use the NIO transport are not affected: **workaround**: use the native transport
(`netty-transport-native-epoll` on the class path and `new VertxOptions().setPreferNativeTransport(true)`): 83 µs against 261 µs.

Ask: bump Netty in Vert.x 5.x once a fixed 4.2.x is released, and maybe mention the workaround in the docs (Vert.x 5 migration notes /
performance notes) until then. The reproducer and the profiles are available.

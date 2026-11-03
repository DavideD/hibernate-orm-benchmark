# Connection Stealing Benchmarks

Everything is on the branch `connection-stealing`, on top of `main` and of the pool steal benchmark by Marco Belladelli,
plus a Netty fix. It tests a pool that waits briefly for a connection on its own event loop, compares Hibernate Reactive
with Hibernate ORM, and traces the higher CPU use of Vert.x 5 to a Netty 4.2 bug.

All measurements use the open-loop benchmark: requests arrive at a fixed rate (Poisson), every request opens a session
and runs 5 `find`s, and latency counts from the scheduled start. CPU per request is the event-loop CPU unless a table
says otherwise. Each line is 2–6 runs with the configurations alternated; ranges show the spread.

| Study | Where | What |
|---|---|---|
| [A pool that waits for a local connection](#a-pool-that-waits-for-a-local-connection) | [`POOL_STEALING.md`](POOL_STEALING.md), section 12 | A Vert.x pool that waits up to 1 ms for a connection of the request's event loop |
| [Hibernate ORM against Hibernate Reactive](#hibernate-orm-against-hibernate-reactive) | [`ORM_VS_REACTIVE.md`](ORM_VS_REACTIVE.md), `run_OrmVsReactive.sh` | Hibernate ORM (platform and virtual threads) against Hibernate Reactive, at the max sustainable rate |
| [Why Vert.x 5 uses more CPU](#why-vertx-5-uses-more-cpu) | [`VERTX5_CPU_PROFILE.md`](VERTX5_CPU_PROFILE.md), [`NETTY_SELECT_ROUNDING.md`](NETTY_SELECT_ROUNDING.md) | async-profiler comparison of Vert.x 5 and 4.5, reproducers and the Netty issue drafts |
| [The Netty fix](#the-netty-fix) | [netty/netty#18138](https://github.com/netty/netty/pull/18138), released in Netty 4.2.20.Final | One-line fix for the NIO select timeout, with tests |

## A pool that waits for a local connection

The earlier results showed that steals happen because the request's own connection is still busy, about 0.3–0.5 ms
from being released, not because releases wait in the pool's queue. So the only way to avoid a steal is to wait for
that connection. `poolType=localWait` does it inside the real Vert.x pool.

- A fork of Vert.x's `SimpleConnectionPool` (`LocalWaitConnectionPool`, for Vert.x 4.5 and 5.1), swapped into the
  shared, Quarkus-like pool by reflection. Idle eviction, max lifetime and the acquisition timeouts keep working.
- A request with no free connection on its event loop waits up to `localWaitUs` (default 1000 µs, a Netty timer on its
  own loop). A released connection goes to its own loop's oldest waiter first, then to the oldest waiter whose wait is
  over. After the wait, the request steals as before.
- Options: `localWaitDepth` (skip the wait when the loop already has many waiters) and `creationAffinity` (open new
  connections on the loop that needs them). Plus `nativeTransport=true` for epoll.
- 20 unit tests without a database: local release preferred, deadline expiry, no double serve, eviction, connection
  failures, and 25 random sequences.

| Setup (Vert.x 5.1, 4 event loops) | `shared` steals | `localWait` steals | CPU vs `shared` | Median latency |
|---|--:|--:|--:|--:|
| Equal load, 1 connection per loop, 2000 req/s | 33% | **1–2%** | −22% | +0.09 ms |
| Equal load, 1 connection per loop, 4000 req/s | 57–62% | **7–10%** | −20% | +0.2 ms |
| Loads 4:2:1:1, 2 connections per loop, 4000 req/s | 27–29% | **2–3%** | −13 to −17% | — |
| All load on one loop, 1 connection per loop, 1000 / 2000 req/s | — | 5–6% / 17–20% | −8 to −12% | +0.17 / +0.3 ms |
| Long sessions (20 finds) | — | half of `shared` | same | same |

`localWait` with the default 1 ms wait. The full tables are in section 12 of [`POOL_STEALING.md`](POOL_STEALING.md).

- **It never does worse than stealing.** With all the load on one loop, `never` collapses (median 1.9–2.3 ms, p99 up
  to 23 ms), while `localWait` still saves CPU. With long sessions it falls back to stealing.
- **No crossover.** With the busiest loop at 42–53%, `localWait` still uses about 10% less CPU.
- **1 ms is the best default.** Waits of 250–500 µs cost 11–16% more CPU than `shared` on Vert.x 5 with NIO, but not on
  4.5 or with epoll; the cause wasn't found. 1.5–2 ms steal a little less, with 4–9% more saving at 4000 req/s only.
- **The transport matters more than the pool:** epoll uses less than half of NIO's CPU.
- For an upstream proposal: it has to be opt-in (it costs 0.1–0.3 ms of median latency), the acquisition timeout and
  queue metrics now include the wait, and pipelining wasn't measured.

<details>
<summary>Commits and how to run</summary>

- 314144b Add localWait, a pool that waits for a connection of the event loop

```
STACK=vertx5 POOL_TYPES="shared preferLocalWait localWait never" ./run_OpenLoopSteal.sh poolSize=4
STACK=vertx5 POOL_TYPES=localWait ./run_OpenLoopSteal.sh poolSize=4 localWaitUs=500 localWaitDepth=1
STACK=vertx5 POOL_TYPES="shared localWait" ./run_OpenLoopSteal.sh poolSize=8 weights=4,2,1,1 nativeTransport=true
```

</details>

## Hibernate ORM against Hibernate Reactive

The same read-only session (5 `find`s by id, no transaction) on both stacks: ORM 7.4.13 with pgjdbc and Agroal, against
Hibernate Reactive 4.5.11 on Vert.x 5.1.10. Same pool size, prepared-statement cache on for both, same open-loop
scheduler. The headline number is the highest rate where p99 stays at or below 10 ms, with the JVM pinned to 2 or 4
cores and PostgreSQL on other cores.

| Max rate, k sessions/s (no DB delay) | 2 cores · pool 8 | 2 cores · pool 32 | 4 cores · pool 8 | 4 cores · pool 32 |
|---|--:|--:|--:|--:|
| ORM, platform threads | 32.3–33.2 | 29.5–30.2 | 59.5 | 60.8 |
| ORM, virtual threads | 18.1–19.8 | 20.2–21.1 | 43.0–45.6 | 48.4–50.6 |
| Reactive `shared`, NIO | 29.1 | 33.0 | 38.8–39.7 | 59.1–60.8 |
| Reactive `shared`, epoll | 30.4 | 32.3–34.8 | 39.7 | 62.4–64.1 |
| Reactive `localWait`, NIO | 31.3–34.2 | 27.4–29.5 | 49.4–60.8 | 52.9–54.0 |
| Reactive `localWait`, epoll | 32.3–33.2 | 26.0–30.2 | 59.5–60.8 | 59.1–64.1 |
| Reactive `perEventLoop`, NIO | 19.8–27.2 | 21.1–22.5 | 62.6–66.4 | 38.2–45.0 |
| Reactive `perEventLoop`, epoll | 22.1–25.3 | 19.7–25.3 | 60.8–66.4 | 39.4–41.6 |

3 runs per cell. With a 1 or 5 ms DB delay the pool is the limit and every stack lands within about 10%.
[`ORM_VS_REACTIVE.md`](ORM_VS_REACTIVE.md) has those tables and the CPU per session.

> **What it shows:** at their best, both stacks reach the same maximum rate within about ±10%. Reactive is a
> CPU-efficiency play, not a throughput play: on epoll it uses 12–23% less CPU per session under load (40–46 µs against
> 50–53 µs at a fixed rate), and it gains 6–11% of max rate when the latency target is relaxed to p99 ≤ 50 ms. It only
> clearly wins on throughput when ORM has far fewer threads than connections (4 threads and 32 connections: 0.4k against
> 5.2k sessions/s at 5 ms delay).

- **Reactive is sensitive to its configuration:** NIO instead of epoll costs 10–40% more CPU, no statement cache about
  70% more, `perEventLoop` with 32 connections drops to 38–45k, and twice as many event loops as cores hurts `shared`
  by 4–24%.
- **Virtual threads were disappointing:** 55–85% of the platform-thread rate at high load, and the most expensive
  option at low rate with a DB delay (120–125 µs against 66–72 µs per session). The cause wasn't found, and tuning was
  minimal.
- **Pipelining had no effect,** as expected: Reactive keeps the connection for the session and sends one statement at
  a time.
- With 2 cores, ORM has the better p99.9 (3.1–5.7 ms against 4.3–13.9 ms for Reactive on epoll); with 4 cores they're
  about equal.
- **Not covered:** writes and transactions, a remote database (more latency, where Reactive's advantages could matter
  more), HikariCP, Quarkus.

<details>
<summary>Commits and how to run</summary>

- 91fce95 Compare Hibernate ORM with Hibernate Reactive

```
./run_OrmVsReactive.sh --help
# On JDK 25 the Gradle 8.4 wrapper doesn't run: use a newer Gradle
GRADLE=gradle JAVA_HOME=/usr/lib/jvm/java-25-openjdk ./run_OrmVsReactive.sh ...
```

</details>

## Why Vert.x 5 uses more CPU

Every request cost far more event-loop CPU on Vert.x 5 than on 4.5, with or without stealing. async-profiler
(`cpu-clock`, JFR) on both stacks under the same load found the cause in Netty 4.2, not in Vert.x, the SQL client or
Hibernate Reactive.

> **Root cause:** Netty 4.2's `NioIoHandler.millisBeforeDeadline` computes the select timeout as
> `(nanosBeforeDeadline + 500_000L) / 1_000_000L`, rounding to the nearest millisecond although its comment says
> "round up". When a timer is less than 0.5 ms away the timeout is 0, so the event loop calls `selectNow()` in a loop
> until the timer is due. Netty 4.1 used `+ 995000L` ("timeout will only be 0 if deadline is within 5 microsecs").
> The 4.2 handler was introduced in netty#13991 from the code before netty#9799 had fixed this in 4.1.
> It's in Netty 4.2.0 to 4.2.19, and fixed in 4.2.20.Final.

| Event-loop CPU per request (4000 req/s, poolSize 4) | Vert.x 4.5 | Vert.x 5 | Vert.x 5 + Netty fix |
|---|--:|--:|--:|
| `perEventLoop`, statement cache off | 102 µs | **297 µs** | 103 µs |
| `perEventLoop`, statement cache on | 58 µs | **107 µs** | 60 µs |
| `shared`, statement cache off | 133 µs | **382 µs** | 150 µs |
| `shared`, statement cache on | 82 µs | **207 µs** | 97 µs |

Unprofiled runs, 2 each. The statement cache halves the CPU on both stacks but doesn't explain the gap.

- **The profile diff:** `SelectorImpl.selectNow` +137 µs and `EPoll.wait` +115 µs per request, about 91% of the gap.
  The I/O work itself is within 7 µs. `epoll_wait` calls go from 9.9 to 79.1 per request while socket reads and writes
  stay the same.
- **Reproduced without Hibernate:** a Vert.x-only PgPool reproducer (`vertx-repro`) and a 35-line Netty-only one
  (`profile/NettyTimerSpin.java`): an event loop with a 1 ms timer and 4000 tasks/s uses 1.1% of a core on Netty 4.1
  and 28% on 4.2.
- **Fixed in Netty 4.2.20.Final** (2026-11-02). Vert.x 5.2.2 still brings 4.2.19; until a Vert.x release has 4.2.20,
  import `io.netty:netty-bom:4.2.20.Final`, or use the native epoll transport, which isn't affected.
- **Unexplained:** with the fix, `shared` runs stay 10–17 µs above Vert.x 4.5.
- `mcp-jafar` was set up for the JFR analysis and agreed with the numbers; the tables come from `jfrconv` collapsed
  stacks and `profile/analyze.py`.

<details>
<summary>Commits and files</summary>

- dbcf54b Profile the event loop CPU of Vert.x 5 against Vert.x 4.5
- 6a88c7d Find the cause of the Vert.x 5 CPU use in Netty 4.2
- 537a767 Measure the fix released in Netty 4.2.20.Final

[`VERTX5_CPU_PROFILE.md`](VERTX5_CPU_PROFILE.md) has the method and the full profile.
[`NETTY_SELECT_ROUNDING.md`](NETTY_SELECT_ROUNDING.md) has the status, the Netty issue and PR drafts, the note for
Vert.x, and how to rerun the reproducers. The profiling scripts are under `profile/` (`profile_run.sh` attaches the
profiler for the measured window only).

</details>

## The Netty fix

Round the select timeout up again, as Netty 4.1 does: `+ 500_000L` becomes `+ 995_000L`, with the same bias in the
overflow guard. The timeout is 0 only when the deadline is within 5 µs. The tests in `NioIoHandlerTest` check the
rounding, the overflow and that a loop with a task 300 µs away doesn't spin; without the fix they fail (`run(...)`
returned 205 times). The transport module passes: 423 tests, 0 failures.

| Case | Netty 4.1 / Vert.x 4.5 | Netty 4.2.19 | With the fix |
|---|--:|--:|--:|
| Netty-only reproducer, 4000 tasks/s (CPU of a core) | 1.1% | **27.9%** | 1.7% |
| select calls in about 6 s | 20,509 | **845,369** | 40,242 |
| Vert.x-only, `perEventLoop`, per request | 96 µs | **260 µs** | 87 µs |
| Hibernate Reactive benchmark, `perEventLoop`, per request | 102 µs | **289 µs** | 107 µs |

| Rerun with the release (2026-11-03, one machine for all columns) | Vert.x 4.5 / Netty 4.1 | Vert.x 5 / Netty 4.2.19 | Vert.x 5 / Netty 4.2.20.Final |
|---|--:|--:|--:|
| Netty-only reproducer, 4000 tasks/s (CPU of a core) | 1.1% | **28.0%** | 1.6–1.7% |
| Vert.x-only, `perEventLoop`, per request | 81 µs | **235 µs** | 87 µs |
| Hibernate Reactive, `perEventLoop`, statement cache off | 107 µs | **283 µs** | 112 µs |
| Hibernate Reactive, `perEventLoop`, statement cache on | 60 µs | **99 µs** | 61 µs |
| Hibernate Reactive, `shared`, statement cache off | 141 µs | **372 µs** | 158 µs |
| Hibernate Reactive, `shared`, statement cache on | 88 µs | **180 µs** | 99 µs |

Vert.x 5.1.10 with all the Netty modules replaced by 4.2.20.Final, 2 unprofiled runs each.
See section 9 of [`NETTY_SELECT_ROUNDING.md`](NETTY_SELECT_ROUNDING.md).

- **The release matches the snapshot:** Vert.x 5 is within 2–7% of Vert.x 4.5 without stealing. With `shared` it stays
  10–17 µs above, and stealing costs 45% more CPU per request at 4000 req/s instead of 33% on Vert.x 4.5 (section 11 of
  [`POOL_STEALING.md`](POOL_STEALING.md)).
- **The trade-off:** timers fire up to 1 ms late again, as on 4.1 (mean lateness of a 1 ms timer 25 → 226 µs,
  p99 423 → 911 µs). In the benchmark the median latency goes back to the Vert.x 4.5 value, about 1.75 ms.
- **Other transports:** epoll, kqueue and io_uring use nanosecond timeouts and aren't affected. The Netty 5.0 branch had
  the same bug; Netty ports 4.2 fixes there automatically.
- **Status:** issue [netty/netty#18137](https://github.com/netty/netty/issues/18137),
  PR [netty/netty#18138](https://github.com/netty/netty/pull/18138) merged into 4.2 on 2026-10-28
  (`06518dc464`, ported to 5.0 by [#18141](https://github.com/netty/netty/pull/18141)), released in
  **Netty 4.2.20.Final** on 2026-11-02. Vert.x has 4.2.20.Final on `master` and `5.2` (next 5.2 release), not yet on `5.1`.

## How far to trust the numbers

The comparisons hold: every one ran on the same machine, with the configurations alternated and repeated, and the large
effects are well above the noise. Absolute microseconds and rates won't carry over to other hardware, and differences
under about 10% or single p99 values shouldn't be relied on.

- **Shared machine:** the runs used an LXC system container (no hypervisor, no CPU quota) on a host shared with other
  containers doing builds. That explains most of the 10–20% spread and the rare stalls.
- **Database network path:** PostgreSQL runs in rootless podman. By default `start_pg.sh` publishes its port through
  `pasta`, a user-space forwarder that adds latency and unmeasured CPU. Use `USE_HOST_NETWORKING=true` to avoid it; the
  ORM comparison used host networking.
- **CPU layout:** Ryzen 9 7950X3D, two 8-core chiplets. The JVM ran on cores 8–15, one chiplet, so a steal here is the
  cheap same-chiplet case; on a server, stealing across chiplets or sockets likely costs more.

---

Measured on Vert.x 4.5.35 and 5.1.10, Netty 4.1.139, 4.2.19 and 4.2.20.Final, Hibernate Reactive 3.4.6 and 4.5.11,
Hibernate ORM 7.4.13, JDK 25.

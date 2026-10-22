# Connection stealing in the Vert.x SQL client pool

"Connection stealing" is what happens when a request running on one Vert.x event loop
gets a database connection that belongs to a different event loop.
Each connection is tied to one thread, so every query then has to cross between two threads.

## 1. Each connection belongs to one event loop

In Vert.x, a TCP socket is registered with a single event loop thread for its whole life.
That thread does all of the connection's I/O: writing the query bytes, reading the response, decoding the rows.

A request also runs on an event loop: in Quarkus, the HTTP request's loop; in this benchmark, the verticle's loop.
When the request's loop and the connection's loop are the same, a query never leaves that thread:

```
event loop A:  request → query → socket write … socket read → decode → continue request
```

## 2. When the pool hands out a connection from another loop

Vert.x's `SimpleConnectionPool` (see the `Acquire` and `Recycle` actions) decides which connection
a request on loop A gets:

1. **A free connection on loop A?** Use it. This is the cheap case.
2. **Pool not full?** Create a new connection. With a plain pool it goes on loop A.
   With `PoolOptions#setEventLoopSize(N)`, which is what Quarkus uses, it goes on the next loop in round-robin,
   which may well not be A.
3. **Pool full, but a connection on loop B is free?** Take it. This is a **steal**.
4. **Nothing free?** Join the wait queue. When any connection is released, it goes to whoever has waited longest,
   whatever loop they're on. Often that's a **steal** too.

## 3. What a steal costs

If the request is on A and the connection is on B, every query crosses threads twice:

```
loop A                          loop B (owns the socket)
request ── schedule command ──▶ write to socket
                                … DB round trip …
                                read + decode rows
continue ◀── hand result back ─
```

Each crossing costs something:

- **Thread handoffs:** a task is queued for the other loop. If that loop is asleep (waiting for I/O),
  it has to be woken up: a system call and a context switch. A steal can cost two of them for every query:
  A wakes up B to send the query, and B wakes up A to hand back the result.
  This is cheapest when both loops are busy, because they're already awake,
  and most expensive when they're lightly loaded and go to sleep between events,
  like a car that keeps stopping and starting in traffic.
- **Cache misses:** B decodes the rows on its CPU core, then A reads them from another core.
- **Uneven load:** B does work for requests it isn't handling. If B is busy, A's query waits behind B's own work,
  which adds latency.

## 4. Why Hibernate Reactive on Quarkus is hit hardest

- **Plain Vert.x apps rarely have this problem.** Each verticle usually creates its own pool,
  so all its connections live on its own loop.
- **Quarkus shares one pool across all loops and spreads connections round-robin**
  (`ReactivePoolUtil` sets `eventLoopSize` to the number of event loops).
  With N event loops, only about 1/N of the connections are local to any request.
  As soon as the pool is busy, steals become likely.
- **Hibernate Reactive keeps the connection for the whole session.**
  A session runs several statements (begin, selects, flush, commit),
  so one steal costs many round trips across threads, not just one.

This was found by Franz (franz1981) and Sanne while profiling Hibernate Reactive in Quarkus:
[quarkusio/quarkus#46817](https://github.com/quarkusio/quarkus/issues/46817).
Metrics for it are tracked in [quarkusio/quarkus#46818](https://github.com/quarkusio/quarkus/issues/46818).

## 5. Why it isn't simply a bug to remove

Stealing is a trade-off. The alternative is a request on A waiting for a connection on A
while connections on other loops sit idle. That means worse latency, and a busy loop can starve.
The options on the table:

- **Wait a little for a local connection before stealing**
  (the idea in [quarkusio/quarkus#46817](https://github.com/quarkusio/quarkus/issues/46817)).
- **Only ever use connections from the same loop, with metrics**
  ([eclipse-vertx/vertx-sql-client#1317](https://github.com/eclipse-vertx/vertx-sql-client/issues/1317)).
- **One pool per event loop**
  (tsegismont's `PerEventLoopPool` in [DavideD/FrameworkBenchmarks#1](https://github.com/DavideD/FrameworkBenchmarks/pull/1)).
  There's never a steal, but the connections are split up: a busy loop can't borrow idle connections from quiet ones.
  The benchmark can run with it, see `poolType` in section 6.

## 6. The benchmark

The `reactive-openloop` module (`OpenLoopStealBenchmark`) runs the requests at a fixed rate,
below the maximum throughput, so that the event loops go to sleep between events, like in most applications.
Each event loop runs a `LoadVerticle` that starts the requests itself, at random intervals (Poisson arrivals),
so no other thread is involved. Each request opens a session and runs 5 `find`s.

It measures:

- **Stolen connections** (`StealCounter`), and how many connections and requests each event loop has.
- **CPU per request** of the event loop threads (`ThreadMXBean`).
- **Sleeps per request** of the event loop threads: the voluntary context switches, from `/proc/thread-self/status`.
  Each time an event loop goes to sleep, something has to wake it up again. Linux only.
- **Latency,** from the time each request was scheduled to start, so a request that starts late counts as slow.

An earlier JMH benchmark started the requests from JMH threads and measured the maximum throughput.
It couldn't show the cost of stealing: every request already went from a JMH thread to an event loop and back,
the same kind of handoff a steal adds, and the database round trip set the throughput.

`poolType` selects the pool:

- `shared` (default): one Vert.x pool for all the event loops.
  `poolEventLoopSize=N` spreads its connections across N event loops like Quarkus does (the default, 4),
  `poolEventLoopSize=0` is a plain Vert.x pool: it opens each connection on the event loop that asks for it.
- `perEventLoop`: one pool per event loop, each with `poolSize / eventLoops` connections (`PerEventLoopPool`).
  A port of tsegismont's pool that works with Vert.x 4 and 5. It never steals.
- `always`, `alwaysActive`, `preferLocalWait`, `never`: the steal policies of section 9.

Run it with `./run_OpenLoopSteal.sh` (see `--help`). It runs every request rate and pool type in its own JVM,
then prints a summary table. By default it uses Hibernate Reactive 3.4.6, Vert.x 4.5.35 and ORM 7.4.13.
`STACK=vertx5` uses Hibernate Reactive 4.5.11, Vert.x 5.1.10 and ORM 7.4.13 (section 11),
`STACK=6.6` uses Hibernate Reactive 2.4.19, Vert.x 4.5.27 and ORM 6.6.50:

```
./run_OpenLoopSteal.sh poolSize=4
RATES="2000 4000" REPEAT=3 POOL_TYPES="shared perEventLoop" ./run_OpenLoopSteal.sh poolSize=8 weights=4,2,1,1
STACK=vertx5 ./run_OpenLoopSteal.sh poolSize=4
```

The setup (dialect detection, schema creation, test data) runs on the main thread,
and Vert.x gives that thread a context on an event loop that isn't the verticles' one.
The connections it opens would stay in the pool and count as steals,
so the benchmark replaces the pool with a fresh one before starting the verticles.

Sections 7 to 10 were measured with Hibernate Reactive 3.4.0, Vert.x 4.5.27 and ORM 7.4.0.
The current versions gave the same results: 3 runs of each, alternating the versions,
at 4000 requests/s with 1 connection per event loop, used the same CPU per request (within 3%) and stole as much.

## 7. When connections are stolen

4 event loops, one 20 s run for each line, Vert.x 4.5.

**8 connections, same load on every event loop:**

| Requests/s | Pool | Stolen | CPU per request | p50 | p99 |
|---|---|---|---|---|---|
| 2000 | plain (`poolEventLoopSize=0`) | 51% | 142 µs | 1.06 ms | 2.1 ms |
| 2000 | Quarkus-like (`poolEventLoopSize=4`) | 6% | 123 µs | 1.02 ms | 1.9 ms |
| 4000 | plain | 45% | 133 µs | 1.18 ms | 2.2 ms |
| 4000 | Quarkus-like | 23% | 131 µs | 1.15 ms | 2.2 ms |

**8 connections, all the requests on one event loop (`weights=1,0,0,0`):**

| Requests/s | Pool | Stolen | CPU per request | Sleeps per request | p50 | p99 |
|---|---|---|---|---|---|---|
| 1000 | plain | 0% | 100 µs | 6.7 | 1.06 ms | 1.9 ms |
| 1000 | Quarkus-like | 17% | 121 µs | 9.9 | 1.08 ms | 2.0 ms |
| 1000 | per event loop | 0% | 103 µs | 8.4 | 1.09 ms | 2.1 ms |
| 2000 | plain | 0% | 90 µs | 3.2 | 1.18 ms | 2.2 ms |
| 2000 | Quarkus-like | 42% | 132 µs | 10.5 | 1.18 ms | 2.1 ms |
| 2000 | per event loop | 0% | 101 µs | 7.2 | 1.32 ms | 2.4 ms |

With 1 connection per event loop (`poolSize=4`), the Quarkus-like pool steals 15% to 63% of the connections
between 1000 and 4000 requests/s (section 8).

What the numbers show:

- **A plain Vert.x pool shared by all the event loops keeps the connections where they were first opened.**
  With the same load everywhere, it stole half of the connections: all 8 were on 2 of the 4 event loops.
  The first verticles to start opened them all, and the pool never moves them.
  Spreading the connections across the event loops (`PoolOptions#setEventLoopSize`, what Quarkus does) avoids this.
- **The Quarkus-like pool steals when an event loop has no free connection of its own:**
  the pool is full, or the requests arrive in bursts, or one event loop has more requests than the others.
  With all the requests on one event loop, (N-1)/N of its connections are on other event loops.
- **One pool per event loop never steals,** but an event loop can only use its share of the connections.
  With all the requests on one event loop it has 2 connections instead of 8: at 2000 requests/s,
  the median latency goes up by 0.14 ms. With more load it would wait longer for a connection.
- **A busy event loop that doesn't steal uses the least CPU:** with all the requests and connections on one event loop,
  it sleeps half as often at 2000 requests/s as at 1000. A busy event loop is often awake already,
  so there's less to wake up.

## 8. The CPU cost of stealing

Results: 4 event loops, 10 s warm-up, 20 s measurement, 3 runs for each line (averages, the runs were within about 3% of each other).
Event loop CPU is per request; "busiest loop" is the CPU of the busiest event loop (100% = one core).

**1 connection per event loop (`poolSize=4`), same load on every event loop:**

| Requests/s | Pool | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 |
|---|---|---|---|---|---|---|---|
| 1000 | shared | 15% | 137 µs | 12.5 | 4% | 0.99 ms | 1.9 ms |
| 1000 | per event loop | 0% | 130 µs | 11.2 | 3% | 1.03 ms | 2.0 ms |
| 2000 | shared | 34% | 147 µs | 13.6 | 8% | 1.08 ms | 2.3 ms |
| 2000 | per event loop | 0% | 125 µs | 10.9 | 6% | 1.13 ms | 2.2 ms |
| 4000 | shared | 63% | 165 µs | 15.0 | 17% | 1.34 ms | 2.6–15.9 ms |
| 4000 | per event loop | 0% | 125 µs | 10.5 | 13% | 1.36 ms | 2.6–2.8 ms |

**2 connections per event loop (`poolSize=8`), loads 4:2:1:1 (`weights=4,2,1,1`):**

| Requests/s | Pool | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 |
|---|---|---|---|---|---|---|---|
| 1000 | shared | 3% | 128 µs | 10.3 | 6% | 0.99 ms | 1.9 ms |
| 1000 | per event loop | 0% | 125 µs | 10.1 | 6% | 1.00 ms | 1.9 ms |
| 2000 | shared | 11% | 125 µs | 9.8 | 10% | 1.04 ms | 2.0 ms |
| 2000 | per event loop | 0% | 118 µs | 9.1 | 11% | 1.06 ms | 2.0 ms |
| 4000 | shared | 33% | 143 µs | 9.2 | 19% | 1.22 ms | 2.3–2.8 ms |
| 4000 | per event loop | 0% | 122 µs | 7.8 | 23% | 1.26 ms | 2.4–2.6 ms |

What the numbers show:

- With stealing, each request uses more event loop CPU, in proportion to how many are stolen:
  about 50–65 µs more for each stolen session of 5 queries, so about 10–13 µs per query.
  At 4000 requests/s with 63% stolen, the event loops use 32% more CPU (0.66 cores instead of 0.50).
- The event loops also sleep more often, and every sleep needs a wake-up: up to 4.5 more per request.
- The event loops are far from busy (at most 23% of a core), and the latency is about the same without stealing:
  the median is at most 0.05 ms higher, the p99 is the same (one run with stealing had a 15.9 ms p99).
- The cost per stolen request was about the same at every rate. All these runs keep the event loops lightly loaded,
  so they don't show whether stealing gets cheaper when the event loops are busy.
- Without stealing, the busiest event loop does all its own work: with uneven load it uses more CPU (23% instead of 19%),
  and it only has its share of the connections. With more uneven load than this, or fewer connections,
  its requests would wait for a connection and the latency would go up (see section 7).
- With enough connections that nothing is stolen (`poolSize=16`, same load everywhere, 2000 requests/s, 0.1% stolen),
  both pools used about the same CPU per request (115 µs shared, 118 µs per event loop, one 10 s run).
  The difference comes from stealing, not from the pool implementation.

## 9. Steal policies

`StealPolicyPool` is a prototype of the options proposed to control stealing.
With no free connection on the request's event loop:

| Policy (`poolType`) | Free connection on another event loop | No free connection anywhere |
|---|---|---|
| `always` | take it | wait for the first connection released by any event loop (what the Vert.x pool does) |
| `alwaysActive` | take it (it's already open) | wait for a connection of the same event loop |
| `preferLocalWait` | wait up to `localWaitMs` (default 1 ms) for a connection of the same event loop, then like `always` | same |
| `never` | wait for a connection of the same event loop | same |

The Vert.x pool can't be configured like this: a released connection always goes to the request that has waited
the longest. So the prototype is made of one Vert.x pool per event loop, all their connections opened at the start,
and decides itself which pool each request gets its connection from (see the class Javadoc).
A connection from another event loop is still acquired on the request's event loop, so the results go back to it,
like with the Vert.x pool. To check it, `always` behaves like the Vert.x pool (`shared`):
same steal ratio, same CPU per request within the noise.

```
POOL_TYPES="shared always alwaysActive preferLocalWait never" ./run_OpenLoopSteal.sh poolSize=4
```

Results with the setups of section 8, 2 runs for each line (ranges show both runs).
These runs were noisier than the ones in section 8: two runs of the same line differ by up to 20%.

**1 connection per event loop (`poolSize=4`), same load on every event loop:**

| Requests/s | Pool | Stolen | CPU per request | Sleeps per request | p50 | p99 |
|---|---|---|---|---|---|---|
| 2000 | `shared` | 34–39% | 145–175 µs | 13.7 | 1.07–1.20 ms | 2.2–5.7 ms |
| 2000 | `always` | 35% | 154–159 µs | 13.7 | 1.10–1.12 ms | 2.5–3.3 ms |
| 2000 | `alwaysActive` | 30–31% | 158–178 µs | 13.3 | 1.13–1.23 ms | 3.1–4.7 ms |
| 2000 | `preferLocalWait` | 1% | 133–135 µs | 11.0 | 1.15–1.17 ms | 2.4–3.2 ms |
| 2000 | `never` | 0% | 133–135 µs | 10.9 | 1.15–1.17 ms | 2.4–3.5 ms |
| 4000 | `shared` | 63–65% | 159–172 µs | 15.0 | 1.32–1.40 ms | 2.8–6.9 ms |
| 4000 | `always` | 64–65% | 170–183 µs | 15.3 | 1.36–1.45 ms | 5.9–12.6 ms |
| 4000 | `alwaysActive` | 37% | 150–155 µs | 13.3 | 1.29–1.33 ms | 2.5–3.9 ms |
| 4000 | `preferLocalWait` | 3–4% | 127–130 µs | 10.8 | 1.34–1.35 ms | 2.4–2.5 ms |
| 4000 | `never` | 0% | 125–127 µs | 10.5 | 1.35 ms | 2.6 ms |

**2 connections per event loop (`poolSize=8`), loads 4:2:1:1:**

| Requests/s | Pool | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 |
|---|---|---|---|---|---|---|---|
| 4000 | `shared` | 32–35% | 138–150 µs | 9.2 | 18–20% | 1.20–1.27 ms | 2.3–4.2 ms |
| 4000 | `always` | 32% | 137–138 µs | 9.0 | 18–19% | 1.19 ms | 2.2–2.3 ms |
| 4000 | `alwaysActive` | 29% | 136–137 µs | 8.8 | 18–19% | 1.18–1.19 ms | 2.2 ms |
| 4000 | `preferLocalWait` | 1% | 120–122 µs | 7.9 | 23% | 1.24–1.25 ms | 2.3–2.4 ms |
| 4000 | `never` | 0% | 119–130 µs | 7.9 | 23–25% | 1.23–1.29 ms | 2.3–3.3 ms |

At 1000 requests/s all the policies were within the noise of each other (15% stolen at most).

What the numbers show:

- `preferLocalWait` with 1 ms gets almost all of the benefit of `never`: a connection of the same event loop
  nearly always gets free within 1 ms, so it steals 1–4% of the time, and uses the same CPU as `never`
  (12–27% less than `always` at 2000 and 4000 requests/s). Unlike `never`, it still steals when a request would wait longer,
  so an event loop can't be stuck waiting while the others have free connections.
- `alwaysActive` only differs from `always` when requests have to wait, which here happens with a full pool:
  at 4000 requests/s with 1 connection per event loop it steals 37% instead of 64%, and uses about 15% less CPU.
  With free connections around, it steals as much as `always`.
- The latency without stealing was the same or better: the median is up to 0.05 ms higher at 2000 requests/s,
  the same at 4000. The p99 of `always` and `shared` had the worst outliers.
- Without stealing, the busiest event loop does more of its own work (23% of a core instead of 18–19%).
  With more uneven load or fewer connections, its requests would wait for a connection: the timeout of
  `preferLocalWait` is what limits that wait.
- `never` is about 5 µs per request slower than `PerEventLoopPool` (section 8), likely the cost of wrapping
  the connections, which all the `StealPolicyPool` policies pay.

## 10. Releases still in the queue of the pool

The Vert.x pool (`SimpleConnectionPool`) processes all its actions (acquire, release, ...) from a single queue,
in the order they're submitted (`CombinerExecutor`). So an acquisition can find no free connection on its event loop,
and steal one or wait, while the release of a connection of its event loop is already in the queue, behind it.
Processing the pending releases before an acquisition would let it see that connection.

`AffinityProbe` measures how often that happens. It replaces the private executor of the pool with one that counts
the releases submitted but not processed yet, for each event loop. When the pool processes an acquisition that finds
no free connection on its event loop, the probe checks whether a release for that event loop is pending.
It also measures how long until the pool processes the release of a connection of that event loop.
A connection released by another event loop (a thief) is submitted to the pool right away, when it's closed,
so the probe sees these releases too.
To check it, a test version that slowed down each acquisition by 200 µs counted 7.9% missed acquisitions.

```
POOL_TYPES=shared ./run_OpenLoopSteal.sh poolSize=4 probe=true
```

Results (shared pool, one 20 s run each). "No local" is the acquisitions that found no free connection
on their event loop, "missed" the ones of those with a release of their event loop pending:

| Setup | Requests/s | Busiest loop | Stolen | No local | Missed | Time to a local release (p50 / p90) |
|---|---|---|---|---|---|---|
| 4 loops, 1 connection each | 1000 | 4% | 15% | 15% | < 0.05% | 374 / 502 µs |
| 4 loops, 1 connection each | 2000 | 8% | 34% | 35% | < 0.05% | 369 / 484 µs |
| 4 loops, 1 connection each | 4000 | 17% | 63% | 72% | < 0.05% | 387 / 500 µs |
| 4 loops, 1 connection each | 8000 | 34% | 75% | 97% | < 0.05% | 290 / 457 µs |
| 4 loops, 2 connections each, loads 4:2:1:1 | 2000 | 11% | 11% | 11% | < 0.05% | 371 / 504 µs |
| 4 loops, 2 connections each, loads 4:2:1:1 | 4000 | 22% | 36% | 37% | < 0.05% | 460 / 645 µs |
| 4 loops, 2 connections each, loads 4:2:1:1 | 8000 | 43% | 66% | 81% | < 0.05% | 410 / 661 µs |
| 8 loops, 1 connection each | 2000 | 6% | 16% | 16% | < 0.05% | 387 / 512 µs |
| 8 loops, 1 connection each | 4000 | 12% | 39% | 39% | < 0.05% | 422 / 562 µs |
| 8 loops, 1 connection each | 8000 | 22% | 79% | 84% | < 0.05% | 477 / 610 µs |

What the numbers show:

- In these workloads, an acquisition almost never misses a release that's already in the queue.
  Processing the pending releases first wouldn't change how often connections are stolen.
- The local connection usually gets free 0.3–0.5 ms later: it's still in use, running the rest of its session.
  That's why `preferLocalWait` with 1 ms (section 9) avoids almost all the steals: it waits for these connections.
- This could be different with other workloads, for example very short sessions or many more event loops,
  where releases and acquisitions happen closer together. The probe can check that.
- The probe adds some work to every action of the pool (about 3% more CPU per request here),
  so it's off by default. Only tested with Vert.x 4.5.

## 11. Vert.x 5

The same runs with Vert.x 5.1.10, Hibernate Reactive 4.5.11 and ORM 7.4.13 (`STACK=vertx5`),
one 20 s run for each line, give the same results.
Every request costs more CPU than with Vert.x 4.5, with or without stealing.

**1 connection per event loop (`poolSize=4`), same load on every event loop:**

| Requests/s | Pool | Stolen | CPU per request | Sleeps per request | p50 | p99 |
|---|---|---|---|---|---|---|
| 2000 | `shared` | 32% | 234 µs | 11.8 | 1.02 ms | 2.0 ms |
| 2000 | `perEventLoop` | 0% | 201 µs | 10.0 | 1.08 ms | 2.0 ms |
| 2000 | `always` | 32% | 232 µs | 11.9 | 1.02 ms | 1.9 ms |
| 2000 | `alwaysActive` | 28% | 216 µs | 11.7 | 1.02 ms | 1.9 ms |
| 2000 | `preferLocalWait` | 0.3% | 204 µs | 10.0 | 1.07 ms | 1.9 ms |
| 2000 | `never` | 0% | 203 µs | 10.1 | 1.06 ms | 1.9 ms |
| 4000 | `shared` | 53% | 300 µs | 10.6 | 1.05 ms | 2.0 ms |
| 4000 | `perEventLoop` | 0% | 230 µs | 8.4 | 1.16 ms | 2.2 ms |
| 4000 | `always` | 54% | 303 µs | 10.7 | 1.06 ms | 2.0 ms |
| 4000 | `alwaysActive` | 37% | 279 µs | 10.0 | 1.11 ms | 2.3 ms |
| 4000 | `preferLocalWait` | 2.1% | 240 µs | 8.3 | 1.17 ms | 2.1 ms |
| 4000 | `never` | 0% | 233 µs | 8.3 | 1.17 ms | 2.2 ms |

- Stealing costs about as much, relatively: 16% more CPU per request at 2000 requests/s, 30% at 4000
  (Vert.x 4.5: 18% and 32%, section 8). In absolute terms it's more: 70 µs per request at 4000 requests/s
  instead of 40 µs.
- The policies compare like with Vert.x 4.5 (section 9).
- The latency is about the same with and without stealing; the median is up to 0.1 ms lower with stealing.
- The probe (section 10) counted no missed acquisitions either (< 0.05%), at 4000 and 8000 requests/s.
  A connection of the same event loop got free 298–358 µs later (median).
- With 8 connections: with the same load everywhere, the plain pool stole 53% and the Quarkus-like pool 19%
  at 4000 requests/s. With all the requests on one event loop, at 2000 requests/s,
  the plain pool stole nothing and the Quarkus-like pool 37% (section 7).
- Vert.x 5.0.12 with Hibernate Reactive 4.4.0 gave the same results, within a few percent.

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
  The benchmark can run with it, see `poolType` below.

## 6. What this means for the benchmark

`ReactivePoolStealBenchmark` counts stolen connections (`StealCounter`) and prints, after each iteration,
the ratio and how many connections and requests each event loop has.
Run it with `./run_PoolStealCounter.sh` (see `--help` for the options, `STACK=vertx5` builds with Vert.x 5).

The `poolType` parameter selects the pool:

- `shared` (default): one Vert.x pool for all the event loops, like Quarkus.
  Add `-p poolEventLoopSize=N` to spread its connections across the loops like Quarkus does.
- `perEventLoop`: one pool per event loop, each with `poolSize / eventLoopCount` connections (`PerEventLoopPool`).
  A port of tsegismont's pool that works with Vert.x 4 and 5. It ignores `poolEventLoopSize`,
  and `poolSize` must be at least `eventLoopCount`.

For example, to compare the three setups with all the requests on one event loop:

```
./run_PoolStealCounter.sh -p poolType=shared -p poolEventLoopSize=0,4
./run_PoolStealCounter.sh -p poolType=perEventLoop -p poolEventLoopSize=0
```

The setup (dialect detection, schema creation, test data) runs on the JMH thread,
and Vert.x gives that thread a context on an event loop that isn't the verticles' one.
The connections it opens stay in the pool, so the benchmark replaces the pool with a fresh one before deploying the verticles.
Without this, all the connections opened during the setup count as steals:
it's what made the plain pool look like it stole 25% of the connections in the third scenario below.

Results from short runs (one fork, 5 s measurement, `sleepMs=1` unless noted):

| Scenario | Stolen | ops/s |
|---|---|---|
| 2 threads, pool 2, 2 loops (original setup) | 0% | 906 |
| Same, with `poolEventLoopSize=2` (Quarkus-like) | 0% | 905 |
| 16 threads, pool 2, 2 loops (pool full) | 49.6% | 919 |
| Same, with `poolEventLoopSize=2` | 50.0% | 916 |
| All requests on loop 0, pool 8, 4 loops, 8 threads, plain pool | 0% | 3,738 |
| Same, with `poolEventLoopSize=4` | 74.9% | 3,703 |
| `sleepMs=0`, requests spread evenly, pool 8, 4 loops, 8 threads | 0% | ~63k |
| Same, with `poolEventLoopSize=4` | 0% | ~63k |
| 16 threads, pool 2, 2 loops, `poolType=perEventLoop` | 0% | 918 |
| All requests on loop 0, pool 8, 4 loops, 8 threads, `poolType=perEventLoop` | 0% | 916 |
| `sleepMs=0`, requests spread evenly, pool 8, 4 loops, 8 threads, `poolType=perEventLoop` | 0% | ~65k |

Vert.x 5 (`STACK=vertx5`) gives the same ratios.

What the numbers show:

- When the requests are spread evenly and there's a free connection, nothing is stolen, with or without the Quarkus setting.
- A full pool causes stealing: the wait queue hands connections out in arrival order, whatever the loop.
- When one loop is busier than the others, the Quarkus setting alone causes stealing:
  (N-1)/N of the connections are on other loops, while the plain pool creates all of them on the busy loop.
- One pool per event loop never steals, and costs nothing when the load is even.
  When one loop does all the work, it only has its share of the connections (2 out of 8 here)
  and throughput drops to a quarter of the shared pool's.
- Throughput hardly changes between 0% and 75% stolen, because the database round trip and the pool size set the limit.
  The cost of stealing is CPU: wake-ups and context switches on both event loops, for every query of the session.
  It's highest when the event loops are lightly loaded and go to sleep between events,
  and it doesn't show up in throughput until the CPU runs out.
  To see it, measure the CPU per request at a fixed request rate, not the maximum throughput (see section 7).

This benchmark can't show the cost of stealing: it measures the maximum throughput,
and every operation already goes from a JMH thread to an event loop and back, the same kind of handoff a steal adds.
Section 7 measures it with a different benchmark.

## 7. Measuring the CPU cost of stealing

The `reactive-openloop` module (`OpenLoopStealBenchmark`) runs the requests at a fixed rate,
well below the maximum throughput, so that the event loops go to sleep between events.
Each event loop runs a `LoadVerticle` that starts the requests itself, at random intervals (Poisson arrivals),
so no other thread is involved. Each request opens a session and runs 5 `find`s.

For the event loop threads only, it measures:

- **CPU per request** (`ThreadMXBean`).
- **Sleeps per request:** the voluntary context switches, from `/proc/thread-self/status`.
  Each time an event loop goes to sleep, something has to wake it up again. Linux only.
- **Latency,** from the time each request was scheduled to start, so a request that starts late counts as slow.

It compares the shared pool with `poolEventLoopSize` equal to the number of event loops (like Quarkus),
which steals, with `PerEventLoopPool`, which can't steal, at the same request rate.
Run it with `./run_OpenLoopSteal.sh` (see `--help`):

```
./run_OpenLoopSteal.sh poolSize=4
RATES="2000 4000" REPEAT=3 ./run_OpenLoopSteal.sh poolSize=8 weights=4,2,1,1
```

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
  its requests would wait for a connection and the latency would go up (see section 6).
- With enough connections that nothing is stolen (`poolSize=16`, same load everywhere, 2000 requests/s, 0.1% stolen),
  both pools used about the same CPU per request (115 µs shared, 118 µs per event loop, one 10 s run).
  The difference comes from stealing, not from the pool implementation.

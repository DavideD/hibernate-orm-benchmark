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

- **Thread handoffs:** a task is queued for the other loop, which often has to be woken up with a system call.
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

## 6. What this means for the benchmark

`ReactivePoolStealBenchmark` counts stolen connections (`StealCounter`) and prints the ratio after each iteration.
Run it with `./run_PoolStealCounter.sh` (see `--help` for the options).

Results from short runs (one fork, 5 s measurement, `sleepMs=1` unless noted):

| Scenario | Stolen | ops/s |
|---|---|---|
| 2 threads, pool 2, 2 loops (original setup) | 0% | 905 |
| Same, with `poolEventLoopSize=2` (Quarkus-like) | 0% | 905 |
| 16 threads, pool 2, 2 loops (pool full) | 48.7% | 911 |
| Same, with `poolEventLoopSize=2` | 49.3% | 917 |
| All requests on loop 0, pool 8, 4 loops, 8 threads, plain pool | 24.8% | 3,711 |
| Same, with `poolEventLoopSize=4` | 74.8% | 3,714 |
| `sleepMs=0`, requests spread evenly, pool 8, 4 loops, 8 threads | 0% | ~63k |

What the numbers show:

- A full pool is what triggers stealing. The Quarkus setting alone does nothing.
- The Quarkus setting makes it worse when one loop is busier than the others:
  about (N-1)/N of the connections end up on other loops.
- Throughput hardly changes between 0% and 75% stolen, because the database round trip and the pool size set the limit.
  The cost of stealing is CPU on the event loops and extra latency.
  It shows up only when the database isn't the bottleneck and the event loops are busy.

Next steps to make the cost visible:

- Generate the load inside the event loops (each verticle keeping N requests in flight)
  instead of blocking JMH threads, which add an event bus round trip to every operation.
- Run several queries per session, like the TechEmpower `/queries?queries=20` endpoint.
- Measure latency and CPU per operation (`-prof perfnorm`) with `sleepMs=0`.

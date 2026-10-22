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
- `localWait`: the shared pool with a forked `SimpleConnectionPool` where a request waits for a connection
  of its own event loop before it can take another one (section 12). `releasesFirst` is the one of section 10.

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

### Processing the releases first

`poolType=releasesFirst` tries the change that would fix this: it's the shared Vert.x pool (Quarkus-like),
with its executor replaced by `ReleasesFirstExecutor`. Releases and the other actions go to separate queues,
and each time the pool runs, it first processes all the pending releases, then the next other action.
An acquisition then sees every connection whose release has been submitted, even after it.
The rest of the pool is unchanged.

It works, but it rarely has anything to do: a test version that counted the reordering found
7 to 68 acquisitions out of 26,000 to 105,000 (0.03–0.06%) that got a release submitted after them first,
at 2000 to 8000 requests/s. The thread that releases a connection usually runs the pool itself, right away,
so the release doesn't wait behind an acquisition. And it changes nothing measurable,
with 1 connection per event loop (`poolSize=4`):

| Version | Requests/s | Pool | Stolen | CPU per request | p50 | p99 |
|---|---|---|---|---|---|---|
| Vert.x 4.5 | 2000 | `shared` | 34–35% | 150–158 µs | 1.09–1.13 ms | 2.3–2.5 ms |
| Vert.x 4.5 | 2000 | `releasesFirst` | 34–35% | 151–155 µs | 1.10 ms | 2.2–2.4 ms |
| Vert.x 4.5 | 8000 | `shared` | 75% | 175–177 µs | 1.86–1.93 ms | 15–28 ms |
| Vert.x 4.5 | 8000 | `releasesFirst` | 75% | 174–177 µs | 1.82–2.06 ms | 5.5–21 ms |
| Vert.x 5.1 | 2000 | `shared` | 32% | 239–241 µs | 1.03 ms | 1.9–2.3 ms |
| Vert.x 5.1 | 2000 | `releasesFirst` | 31% | 229–230 µs | 1.01–1.02 ms | 1.9 ms |
| Vert.x 5.1 | 4000 | `shared` | 53–54% | 305–310 µs | 1.06–1.07 ms | 2.0–2.1 ms |
| Vert.x 5.1 | 4000 | `releasesFirst` | 53% | 304–308 µs | 1.06 ms | 2.0 ms |
| Vert.x 5.1 | 8000 | `shared` | 70% | 306–307 µs | 1.29–1.30 ms | 7.4–9.0 ms |
| Vert.x 5.1 | 8000 | `releasesFirst` | 69–70% | 305–306 µs | 1.28–1.29 ms | 4.6–5.6 ms |

2 runs for each line, 3 for Vert.x 4.5 at 8000 requests/s (alternating the pools).
The Vert.x 4.5 runs at 4000 requests/s and above had large latency outliers with both pools on that day.
The probe (it works on top of `releasesFirst`) counted no missed acquisitions with either pool.

To avoid the steals, an acquisition has to wait for a connection of its event loop that's still in use,
like `preferLocalWait` (section 9) does, not only see the releases that are already submitted.

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

## 12. Waiting for a local connection inside the Vert.x pool

`StealPolicyPool` (section 9) shows what a policy can do, but it isn't the Vert.x pool.
It is made of one pool per event loop, all its connections are opened at the start
(so no idle eviction or maximum lifetime), every connection is wrapped (about 5 µs per request),
and the wait uses Vert.x timers (1 ms resolution).
`poolType=localWait` puts the same idea inside the Vert.x pool: it is the shared, Quarkus-like pool
(`poolEventLoopSize=N`) with a forked `SimpleConnectionPool`.

### How the fork is plugged in

The Vert.x SQL client pool is made of these layers (Vert.x 4.5 and 5.1 have the same ones):

```
Pool (PgPoolImpl in 4.5) → PoolImpl → SqlConnectionPool → SimpleConnectionPool → CombinerExecutor
                                      (idle eviction,     (waiters, slots,       (runs the actions
                                       max lifetime,       actions: Acquire,      one at a time)
                                       event loop size)    Recycle, Remove, ...)
```

`ReleasesFirstExecutor` and `AffinityProbe` replace the private `sync` field of `SimpleConnectionPool`: its executor.
That is enough to reorder the actions or to count them, but not to change which waiter a released connection goes to.
That is decided inside the actions (`Acquire`, `Recycle`) and by the queue of waiters, both private to the class.

So `LocalWaitPool.createPool` creates a normal pool with `Pool.pool(...)`, and replaces the `pool` field of its
`SqlConnectionPool` (a private final field, set by reflection before the pool is used) with a
`LocalWaitConnectionPool`. The fork takes the `connector`, the maximum size, the maximum wait queue size
and the context provider (the round-robin of `eventLoopSize`) of the pool it replaces.
Everything around it is still the Vert.x code: the idle eviction (`SqlConnectionPool#evict` calls `ConnectionPool#evict`),
the maximum lifetime, the pipelining, the timeouts of `getConnection`, and what happens when a connection fails
(`ConnectFailed` and `Remove`, copied).

The fork is a copy of `SimpleConnectionPool` with its license header, in
`src/vertx5/java/io/vertx/core/internal/pool` (Vert.x 5.1) and `src/vertx4/java/io/vertx/core/net/impl/pool` (Vert.x 4.5),
the same packages as the original. It has to be: the constructor and the fields of `PoolWaiter` are package private,
and the pool hands `PoolWaiter` objects to `SqlConnectionPool`. That works on the class path, not with modules.
A change in Vert.x itself wouldn't need any of this.

### What is different from `SimpleConnectionPool`

| Action | `SimpleConnectionPool` | `LocalWaitConnectionPool` |
|---|---|---|
| `Acquire`, free connection on the event loop of the request | use it | same |
| `Acquire`, pool not full | open a connection on the next event loop of the round-robin | same. With `creationAffinity`: on the event loop of the request while it has fewer than `maxSize / N` connections, else on an event loop that has fewer than that (`connectionContext`) |
| `Acquire`, pool full | take a free connection of any event loop (a steal), else join the queue | **wait for a connection of the event loop**: join the local queue of the event loop, with a deadline (`Acquire#execute`, step 3). Don't take the free connection of another event loop. The wait is skipped (steal at once, else join the general queue) if the event loop has no connection, or more than `localWaitDepth` × its connections local waiters |
| `Recycle` | the connection goes to the waiter that has waited the longest, whatever its event loop | the oldest local waiter of the event loop of the connection, else the oldest waiter whose wait is over, else it stays free (`pollFor`) |
| deadline (new action, `Expire`) | | the waiter takes a free connection of any event loop if there is one, else joins the queue of the waiters whose wait is over, served by the first connection released by any event loop |
| `Remove`: a connection is removed and a waiter can open another | the oldest waiter | the oldest waiter of any queue (`pollOldest`) |

The deadline is a Netty timer of the event loop of the waiter (`EventLoop#schedule`), started after the action
and cancelled when the waiter is served, cancelled, or the pool is closed. It only submits an `Expire` action to the pool,
and the action does nothing if the waiter isn't in a local queue any more. A waiter is in at most one queue,
and the actions are run one at a time, so a waiter can't be served twice or be forgotten.

Options of `run_OpenLoopSteal.sh` for `poolType=localWait`: `localWaitUs` (default 1000, see the results),
`localWaitDepth` (default 0: always wait) and `creationAffinity` (default true).
The benchmark also prints what happened to the waiters (the `Local wait:` lines):
how many waited, were served by a local release, took a free connection or joined the queue when their wait ended,
and how late their wait ended compared to the deadline.

```
POOL_TYPES="shared preferLocalWait localWait never" STACK=vertx5 ./run_OpenLoopSteal.sh poolSize=4 localWaitUs=1000
```

### Results

The runs of this section: Vert.x 5.1.10 and Hibernate Reactive 4.5.11 (`STACK=vertx5`, NIO), 4 event loops,
10 s warm-up, 20 s measurement. JDK 25 on a Ryzen 9 7950X3D, the JVM pinned to the 8 cores of one core complex
(`taskset -c 8-15`: one thread per core), PostgreSQL in a rootless podman container.
The earlier sections ran on other versions of the JDK and on another machine, so the CPU per request is higher here:
compare the lines of one table, not the tables with the earlier ones.
The pools were alternated between runs, in a different order every round. The ranges show the lowest and the highest run,
"Runs" is the number of runs of the line (the lines with more runs were repeated in other tables).
"Waits ended by a local release" is the part of the requests that waited for a local connection that got one,
the others took a free connection of another event loop or joined the queue when their wait ended.

**1 connection per event loop (`poolSize=4`), same load on every event loop:**

| Requests/s | Pool | Runs | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|---|---|
| 2000 | `shared` | 6 | 32.4–33.4% | 306–334 µs | 11.8–11.9 | 18–19% | 1.12–1.14 ms | 2.0–2.8 ms |  |
| 2000 | `preferLocalWait` 1 ms | 3 | 1.5–4.1% | 243–308 µs | 9.4–9.5 | 12–16% | 1.21–1.29 ms | 2.2–7.6 ms |  |
| 2000 | `localWait` 250 µs | 3 | 28.3–29.2% | 362–370 µs | 10.1 | 21% | 1.19–1.20 ms | 2.3–3.8 ms | 11% |
| 2000 | `localWait` 500 µs | 3 | 9.4–11.1% | 340–375 µs | 7.9 | 18–20% | 1.21–1.24 ms | 2.7–3.9 ms | 59–65% |
| 2000 | `localWait` 750 µs | 3 | 4.6–5.1% | 293–310 µs | 8.8 | 15–16% | 1.22–1.24 ms | 2.4–2.6 ms | 81–83% |
| 2000 | `localWait` 1000 µs | 6 | 1.1–2.2% | 232–272 µs | 9.5–9.6 | 12–14% | 1.21–1.24 ms | 2.1–2.6 ms | 92–96% |
| 2000 | `localWait` 1500 µs | 3 | 0.2–0.5% | 236–250 µs | 9.5–9.6 | 12–13% | 1.22–1.23 ms | 2.2–2.4 ms | 98–99% |
| 2000 | `localWait` 2000 µs | 3 | 0.1–1.6% | 228–320 µs | 9.3–9.6 | 12–16% | 1.21–1.32 ms | 2.2–5.4 ms | 94–99% |
| 2000 | `never` | 3 | 0.0% | 253–282 µs | 9.4–9.5 | 13–14% | 1.23–1.27 ms | 2.3–4.8 ms |  |
| 4000 | `shared` | 6 | 57.2–62.0% | 372–410 µs | 9.9–10.3 | 40–43% | 1.20–1.38 ms | 2.4–44.3 ms |  |
| 4000 | `preferLocalWait` 1 ms | 3 | 8.3–11.6% | 318–341 µs | 7.7 | 32–34% | 1.41–1.47 ms | 3.0–8.8 ms |  |
| 4000 | `localWait` 250 µs | 3 | 44.4–44.8% | 431–445 µs | 7.3–7.4 | 46–48% | 1.28–1.31 ms | 2.7–3.4 ms | 19% |
| 4000 | `localWait` 500 µs | 3 | 23.0–24.5% | 446–463 µs | 5.0 | 47–48% | 1.34–1.36 ms | 3.4–4.1 ms | 48–50% |
| 4000 | `localWait` 750 µs | 3 | 14.2–15.3% | 360–377 µs | 6.6 | 38–39% | 1.38–1.41 ms | 2.8–4.1 ms | 70–71% |
| 4000 | `localWait` 1000 µs | 6 | 7.3–9.7% | 298–328 µs | 7.7–7.8 | 30–34% | 1.38–1.44 ms | 2.6–7.2 ms | 81–86% |
| 4000 | `localWait` 1500 µs | 3 | 2.4–2.6% | 293–302 µs | 7.2 | 30–31% | 1.40–1.41 ms | 2.8–2.9 ms | 95% |
| 4000 | `localWait` 2000 µs | 3 | 0.7–0.8% | 281–284 µs | 7.5–7.6 | 29% | 1.40–1.41 ms | 2.9 ms | 98% |
| 4000 | `never` | 3 | 0.0% | 291–303 µs | 7.4–7.5 | 29–31% | 1.42–1.45 ms | 3.5–4.8 ms |  |

What the numbers show:

- **`localWait` with 1000 µs does what `preferLocalWait` did, inside the Vert.x pool.**
  It steals 1–2% at 2000 requests/s and 7–10% at 4000 requests/s (`shared`: 33% and 57–62%),
  and uses 22% and 20% less CPU than `shared` (averages of the runs), where `never` uses 17% and 24% less.
  The event loops also sleep 19–24% less.
  `preferLocalWait` gives the same results within the noise, with the wrapping of the connections.
- **The wait has to be about as long as the local connection stays busy.** With 250 µs, 80–90% of the waits end without
  a local release: the request waits and then steals anyway. With 500 µs it's 35–50%, with 750 µs 17–30%,
  with 1000 µs 5–19%. With Vert.x 5 and NIO, the short waits use *more* CPU than `shared`:
  +11–15% with 250 µs and +14–16% with 500 µs, while 750 µs is 5–6% below `shared`.
  The deadline itself isn't late (the median is 1–2 µs late, the p99 25–330 µs with 500 µs),
  and the same short waits didn't cost anything with Vert.x 4.5 or with epoll (see below).
  I haven't found why.
- **A longer wait removes the steals that are left:** at 2000 requests/s 1500 µs and 2000 µs steal 0.2–0.5% and 0.1–1.6%,
  at 4000 requests/s 2.4–2.6% and 0.7–0.8% (1000 µs: 7–10%). The CPU is the same at 2000 requests/s,
  and 4–9% lower than with 1000 µs at 4000 (281–302 µs).
- **The median latency goes up** by about 0.09 ms at 2000 requests/s and 0.2 ms at 4000, like `never`:
  a request that waits for its own connection doesn't use the free one of another event loop.
  The p99 isn't worse than `shared` (and has fewer outliers).
- The end of the wait is on time for the median request but not for all of them: with 1000 µs the p99 is
  0.06–0.9 ms late at these rates, and up to 2.4 ms with busier event loops (below). An event loop checks its timers
  between two tasks.

### Skewed load, where waiting could hurt

**All the requests on one event loop (`weights=1,0,0,0`), 1 connection per event loop:**

| Requests/s | Pool | Runs | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | `shared` | 3 | 41.9–42.2% | 270–290 µs | 13.8–14.0 | 21–23% | 1.14–1.18 ms | 2.0–3.2 ms |  |
| 1000 | `preferLocalWait` 1 ms | 3 | 5.2–6.2% | 245–253 µs | 8.3 | 24–25% | 1.31–1.33 ms | 2.3–2.9 ms |  |
| 1000 | `localWait` 500 µs | 3 | 15.6–18.4% | 321–344 µs | 6.7–7.1 | 30–32% | 1.25–1.29 ms | 2.2–3.1 ms | 57–63% |
| 1000 | `localWait` 1000 µs | 3 | 5.0–6.5% | 241–256 µs | 8.3 | 23–25% | 1.31–1.34 ms | 2.4–3.2 ms | 87–90% |
| 1000 | `never` | 3 | 0.0% | 242–247 µs | 7.7 | 24–25% | 1.34–1.36 ms | 2.8–3.4 ms |  |
| 2000 | `shared` | 3 | 58.4–58.6% | 270–273 µs | 14.3 | 38–39% | 1.19–1.20 ms | 2.1–2.3 ms |  |
| 2000 | `preferLocalWait` 1 ms | 3 | 17.7–18.5% | 247–251 µs | 8.2 | 45% | 1.48–1.50 ms | 2.6–3.2 ms |  |
| 2000 | `localWait` 500 µs | 3 | 32.8–35.3% | 335–344 µs | 6.7–7.1 | 59% | 1.32–1.36 ms | 2.3–6.0 ms | 42–45% |
| 2000 | `localWait` 1000 µs | 3 | 17.2–20.2% | 245–253 µs | 8.2–8.4 | 44–45% | 1.47–1.54 ms | 2.6–3.5 ms | 74–77% |
| 2000 | `never` | 3 | 0.0% | 242–254 µs | 5.6–5.8 | 48–50% | 1.87–2.25 ms | 7.9–22.8 ms |  |

**2 connections per event loop, loads 4:2:1:1 (`poolSize=8 weights=4,2,1,1`):**

| Requests/s | Pool | Runs | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|---|---|
| 4000 | `shared` | 3 | 26.7–27.4% | 308–320 µs | 8.4–8.5 | 44–46% | 1.21–1.23 ms | 2.8–9.5 ms |  |
| 4000 | `preferLocalWait` 1 ms | 3 | 3.0–3.2% | 265–274 µs | 6.9–7.0 | 43–44% | 1.29–1.30 ms | 3.1–5.3 ms |  |
| 4000 | `localWait` 500 µs | 3 | 12.0–13.4% | 311–327 µs | 5.7 | 52–55% | 1.26–1.29 ms | 2.8–4.6 ms | 48–52% |
| 4000 | `localWait` 1000 µs | 3 | 2.2–2.6% | 260–264 µs | 7.0 | 42–43% | 1.27–1.28 ms | 2.4–3.4 ms | 90–92% |
| 4000 | `never` | 3 | 0.0% | 260–263 µs | 6.9 | 43–44% | 1.28 ms | 2.6–2.9 ms |  |

What the numbers show:

- **With all the load on one event loop, the wait costs latency:** that loop has 1 connection, and keeps its requests
  waiting for it. The median goes up by 0.17 ms at 1000 requests/s and 0.3 ms at 2000, the CPU is 8–12% lower than `shared`,
  and 17–20% of the connections are still stolen at 2000 requests/s (the waits that run out).
  `never` is the case that hurts: the only local connection is saturated, with a 1.87–2.25 ms median and a p99 of up to 22.8 ms.
  The deadline limits that to a p99 of 2.6–3.5 ms.
- **With 500 µs the wait is the worst choice here:** 19–28% more CPU than `shared`, and a latency between `shared` and 1000 µs.
- With 2 connections per event loop and uneven load, 1000 µs steals 2–3% instead of 27–29%, with 13–17% less CPU than `shared`
  and a 0.05 ms higher median.

### Longer sessions

The sessions run 20 `find`s instead of 5 (`queriesPerSession=20`), so a connection is busy about 4 times longer and
the local connection often gets free after the deadline. The rates are lower to keep the connections as busy as before.

**1 connection per event loop (`poolSize=4 queriesPerSession=20`), same load on every event loop:**

| Requests/s | Pool | Runs | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|---|---|
| 400 | `shared` | 3 | 16.7–17.4% | 700–709 µs | 43.4–43.7 | 8% | 2.38–2.44 ms | 3.4–3.7 ms |  |
| 400 | `preferLocalWait` 1 ms | 3 | 7.8–10.2% | 712–754 µs | 39.8–40.3 | 7–8% | 2.47–2.57 ms | 4.3–8.2 ms |  |
| 400 | `localWait` 250 µs | 3 | 15.7–16.6% | 732–742 µs | 42.2–42.4 | 8% | 2.43–2.52 ms | 3.5–4.7 ms | 7–9% |
| 400 | `localWait` 500 µs | 3 | 14.4–15.0% | 760–774 µs | 41.0–41.1 | 8–9% | 2.46–2.47 ms | 3.8–4.2 ms | 14–16% |
| 400 | `localWait` 1000 µs | 3 | 7.3–8.9% | 709–728 µs | 39.7–39.9 | 8% | 2.44–2.53 ms | 3.9–6.4 ms | 52–57% |
| 400 | `never` | 3 | 0.0% | 659–676 µs | 38.2–38.3 | 7% | 2.46–2.55 ms | 5.0–6.6 ms |  |
| 600 | `shared` | 3 | 27.2–28.3% | 769–792 µs | 44.4–44.5 | 13–14% | 2.48–2.50 ms | 3.8–5.9 ms |  |
| 600 | `preferLocalWait` 1 ms | 3 | 12.8–13.9% | 778–783 µs | 38.9–39.2 | 12% | 2.55–2.59 ms | 4.3–4.9 ms |  |
| 600 | `localWait` 250 µs | 3 | 24.6–28.5% | 812–875 µs | 42.2–42.8 | 14–15% | 2.51–2.63 ms | 3.9–13.3 ms | 9–10% |
| 600 | `localWait` 500 µs | 3 | 22.6–24.8% | 858–902 µs | 40.2–40.5 | 14–15% | 2.58–2.64 ms | 4.4–10.2 ms | 16–17% |
| 600 | `localWait` 1000 µs | 3 | 13.0–14.7% | 766–802 µs | 39.0–39.3 | 13% | 2.55–2.60 ms | 4.4–7.8 ms | 48–52% |
| 600 | `never` | 3 | 0.0% | 682–697 µs | 36.5–36.6 | 11% | 2.53–2.60 ms | 6.0–8.1 ms |  |

What the numbers show:

- **With 1000 µs the pool degrades to `shared`:** only 48–57% of the waits end with a local release,
  it steals half as much (7–9% and 13–15%, `shared` 17% and 27–28%), and uses the same CPU and the same latency within the noise.
- **With 250 µs and 500 µs it's a loss:** only 7–17% of the waits end with a local release,
  the CPU is 4–13% higher than `shared`, and the median 0.05–0.15 ms higher. The wait is wasted.

### Queue depth shortcut and creation affinity

`localWaitDepth=k` skips the wait when the event loop already has more than k × its connections local waiters.
`creationAffinity=false` goes back to the round-robin of Vert.x for the new connections.
All with `localWaitUs=1000`, 3 runs for each line:

| Setup | Variant | Runs | Stolen | CPU per request | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|
| poolSize=4, 4000/s | `shared` | 3 | 57.0–57.2% | 373–378 µs | 1.19–1.20 ms | 2.3–2.5 ms |  |
| poolSize=4, 4000/s | `localWait` 1000 µs | 3 | 7.3–7.9% | 299–306 µs | 1.38–1.40 ms | 2.8–3.1 ms | 84–85% |
| poolSize=4, 4000/s | + `localWaitDepth=1` | 3 | 7.8–10.1% | 292–316 µs | 1.33–1.38 ms | 2.4–10.0 ms | 90–94% |
| poolSize=4, 4000/s | + `localWaitDepth=2` | 3 | 7.4–9.1% | 298–323 µs | 1.37–1.41 ms | 2.6–3.7 ms | 85–88% |
| poolSize=4, 4000/s | + `creationAffinity=false` | 3 | 6.9–9.8% | 296–326 µs | 1.38–1.44 ms | 2.6–6.2 ms | 81–86% |

| Setup | Variant | Runs | Stolen | CPU per request | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|
| poolSize=4, weights=1,0,0,0, 2000/s | `shared` | 3 | 58.3–58.8% | 262–280 µs | 1.18–1.22 ms | 2.1–4.1 ms |  |
| poolSize=4, weights=1,0,0,0, 2000/s | `localWait` 1000 µs | 3 | 16.7–22.6% | 241–260 µs | 1.45–1.55 ms | 2.5–8.7 ms | 71–78% |
| poolSize=4, weights=1,0,0,0, 2000/s | + `localWaitDepth=1` | 3 | 20.9–23.8% | 240–253 µs | 1.31–1.36 ms | 2.4–4.0 ms | 94–98% |
| poolSize=4, weights=1,0,0,0, 2000/s | + `localWaitDepth=2` | 3 | 17.0–21.6% | 238–255 µs | 1.38–1.45 ms | 2.5–4.7 ms | 81–87% |
| poolSize=4, weights=1,0,0,0, 2000/s | + `creationAffinity=false` | 3 | 17.1–24.4% | 243–267 µs | 1.46–1.61 ms | 2.8–6.2 ms | 69–78% |

| Setup | Variant | Runs | Stolen | CPU per request | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|
| poolSize=8, weights=4,2,1,1, 4000/s | `shared` | 3 | 27.3–29.1% | 313–326 µs | 1.22–1.26 ms | 3.6–5.3 ms |  |
| poolSize=8, weights=4,2,1,1, 4000/s | `localWait` 1000 µs | 3 | 3.3–4.6% | 266–287 µs | 1.30–1.33 ms | 4.5–5.3 ms | 85–88% |
| poolSize=8, weights=4,2,1,1, 4000/s | + `localWaitDepth=1` | 3 | 3.4–4.5% | 270–278 µs | 1.27–1.30 ms | 2.5–5.1 ms | 91–94% |
| poolSize=8, weights=4,2,1,1, 4000/s | + `localWaitDepth=2` | 3 | 3.3–7.2% | 271–306 µs | 1.30–1.41 ms | 3.9–8.3 ms | 80–89% |
| poolSize=8, weights=4,2,1,1, 4000/s | + `creationAffinity=false` | 3 | 3.1–8.1% | 272–306 µs | 1.30–1.42 ms | 3.7–24.1 ms | 76–89% |

What the numbers show:

- **The shortcut helps the event loop that has all the load:** with `localWaitDepth=1` the median is 1.31–1.36 ms instead of 1.45–1.55 ms,
  at the same CPU (240–253 µs) and about the same steals. Everywhere else it changes nothing within the noise.
  `localWaitDepth=2` is in between. It's 3 runs for each line: the one setup where it matters is one where the ranges don't overlap.
- **Creation affinity changes nothing after the warm-up,** in these setups: the pool opens all its connections in the first
  second, and the round-robin already gives every event loop its share when all of them ask for connections.
  It could matter when the first requests all come from one event loop. (A first version of the fork that only used the event
  loop of the request below its share, and the round-robin otherwise, ended up with 0, 2, 2, 0 connections on the 4 event loops,
  so now it uses an event loop that is under its share before it falls back to the round-robin.)
- The same options with 500 µs didn't change anything either, and `localWaitUs=0` (the fork and the creation affinity, without waiting)
  uses the same CPU and steals as much as `shared` (373–399 µs and 57–60% against 369–375 µs and 57%, same load on 4 event loops):
  the fork itself costs nothing measurable.

### A busier event loop

Is there a load where stealing gets cheaper than waiting, because the event loops are awake anyway?
Same load on every event loop, with more connections to sustain more requests: the busiest event loop is 27–33% busy at 4000 requests/s
with 2 connections per event loop, and 42–53% busy at 6000 requests/s.

| Requests/s | Pool | Runs | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|---|---|
| 4000 | `shared` | 3 | 18.3–18.9% | 312–313 µs | 8.3 | 32–33% | 1.19–1.20 ms | 2.1–3.1 ms |  |
| 4000 | `localWait` 1000 µs | 3 | 0.5–0.7% | 264–276 µs | 7.5 | 27–28% | 1.22–1.23 ms | 2.1–2.2 ms | 96–97% |
| 4000 | `localWait` 2000 µs | 3 | 0.0–0.1% | 264–279 µs | 7.4–7.5 | 27–28% | 1.23–1.24 ms | 2.2–2.3 ms | 99–100% |
| 4000 | `never` | 3 | 0.0% | 269–276 µs | 7.5 | 27–28% | 1.23–1.24 ms | 2.1–2.5 ms |  |
| 6000 | `shared` | 3 | 38.3–49.5% | 323–343 µs | 7.0–7.4 | 50–53% | 1.30–1.56 ms | 2.6–29.3 ms |  |
| 6000 | `localWait` 1000 µs | 3 | 3.3–10.7% | 285–314 µs | 5.8–6.2 | 44–47% | 1.38–1.61 ms | 2.7–58.0 ms | 76–90% |
| 6000 | `localWait` 2000 µs | 3 | 0.5–3.3% | 279–311 µs | 5.6–6.1 | 42–47% | 1.38–1.58 ms | 3.3–17.9 ms | 92–98% |
| 6000 | `never` | 3 | 0.0% | 278–283 µs | 6.1 | 42–43% | 1.36–1.39 ms | 2.9–6.0 ms |  |

| Requests/s | Pool | Runs | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|---|---|
| 6000 | `shared` | 3 | 8.3–9.7% | 285–292 µs | 5.9–6.0 | 43–44% | 1.33–1.36 ms | 3.1–21.6 ms |  |
| 6000 | `localWait` 1000 µs | 3 | 1.4–2.1% | 281–284 µs | 5.7 | 42–43% | 1.34–1.38 ms | 5.5–14.8 ms | 76–80% |
| 6000 | `localWait` 2000 µs | 3 | 0.1–1.4% | 282–293 µs | 5.4–5.7 | 43–44% | 1.35–1.45 ms | 3.0–10.0 ms | 87–98% |
| 6000 | `never` | 3 | 0.0% | 286–290 µs | 5.5–5.7 | 43–44% | 1.37–1.41 ms | 4.1–14.6 ms |  |

What the numbers show:

- **No crossover:** with 2 connections per event loop, `localWait` uses 14% less CPU than `shared` at 4000 requests/s
  (27–33% busy) and 10% less at 6000 (42–53% busy). Stealing does seem to get cheaper as the event loops get busier:
  the CPU saved by each avoided steal (the difference between `shared` and `localWait` per request, divided by the steals that
  were avoided, averages of the runs) is 85–90 µs with the event loops 44–52% busy, against 150–250 µs with them 18–44% busy.
  It's not a clean trend, as the setups differ, and it's still positive everywhere.
- With 4 connections per event loop, `shared` only steals 8–10% at 6000 requests/s, and all the pools use the same CPU
  (281–293 µs). Waiting has nothing to save there, and costs nothing.
- At 6000 requests/s some runs of every pool have latency outliers (p99 of 15–58 ms), `never` too.

### Vert.x 4.5

The same setup as the first table, 2 runs for each line, Vert.x 4.5.35 with Hibernate Reactive 3.4.6 (the default stack, NIO):

| Requests/s | Pool | Runs | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|---|---|
| 2000 | `shared` | 2 | 37.9–41.5% | 128–151 µs | 14.3–14.5 | 7–8% | 1.24–1.34 ms | 2.4–6.9 ms |  |
| 2000 | `preferLocalWait` 1 ms | 2 | 4.7–4.9% | 129–135 µs | 11.3 | 6–7% | 1.40–1.42 ms | 5.5–6.6 ms |  |
| 2000 | `localWait` 500 µs | 2 | 10.0–11.7% | 119–123 µs | 11.8–12.0 | 6–7% | 1.31–1.34 ms | 2.8–3.0 ms | 62–66% |
| 2000 | `localWait` 1000 µs | 2 | 1.9–3.2% | 111–121 µs | 11.0–11.1 | 6% | 1.32–1.36 ms | 2.6–4.2 ms | 90–94% |
| 2000 | `never` | 2 | 0.0% | 112–127 µs | 10.8 | 6% | 1.32–1.40 ms | 2.7–8.2 ms |  |
| 4000 | `shared` | 2 | 67.7–68.7% | 146–150 µs | 16.1 | 15–16% | 1.47–1.53 ms | 10.4–53.8 ms |  |
| 4000 | `preferLocalWait` 1 ms | 2 | 13.7–14.0% | 132–134 µs | 11.7 | 13–14% | 1.68–1.69 ms | 9.3–29.8 ms |  |
| 4000 | `localWait` 500 µs | 2 | 27.7–27.8% | 124 µs | 13.0 | 13% | 1.52–1.53 ms | 3.0–3.7 ms | 50% |
| 4000 | `localWait` 1000 µs | 2 | 9.6–10.1% | 112–117 µs | 11.3 | 12% | 1.60–1.61 ms | 3.2–3.8 ms | 83% |
| 4000 | `never` | 2 | 0.0% | 117–126 µs | 10.4 | 12–13% | 1.70–1.84 ms | 8.2–10.1 ms |  |

The pools compare like with Vert.x 5. The CPU per request is much lower (111–151 µs).
Here 500 µs already uses less CPU than `shared` (119–124 µs against 128–151 µs), and 1000 µs is as good as `never`.

### Native epoll transport

The NIO selector of Netty only has a resolution of 1 ms, which could matter for waits of less than 1 ms,
and the CPU per request of Vert.x 5 is high with it. So this is the same run on the native epoll transport,
`EPOLL=4.2.19.Final nativeTransport=true` (`STACK=vertx5`, 2 runs for each line):

| Requests/s | Pool | Runs | Stolen | CPU per request | Sleeps per request | Busiest loop | p50 | p99 | Waits ended by a local release |
|---|---|---|---|---|---|---|---|---|---|
| 2000 | `shared` | 2 | 29.7–30.9% | 120–128 µs | 13.9–14.0 | 7% | 1.04–1.07 ms | 1.6–2.7 ms |  |
| 2000 | `localWait` 250 µs | 2 | 25.5–27.2% | 120–134 µs | 13.9 | 7–8% | 1.11–1.17 ms | 1.8–3.8 ms | 12% |
| 2000 | `localWait` 500 µs | 2 | 6.6% | 115 µs | 11.8 | 6% | 1.16–1.17 ms | 2.1–2.8 ms | 72–73% |
| 2000 | `localWait` 1000 µs | 2 | 2.1–2.6% | 116–122 µs | 11.3–11.4 | 6% | 1.19–1.21 ms | 3.4–3.9 ms | 90–92% |
| 2000 | `never` | 2 | 0.0% | 114–120 µs | 11.1–11.2 | 6% | 1.18–1.21 ms | 2.9–4.8 ms |  |
| 4000 | `shared` | 2 | 58.2–59.8% | 151–156 µs | 15.6–15.7 | 16% | 1.26–1.31 ms | 7.4–20.6 ms |  |
| 4000 | `localWait` 250 µs | 2 | 44.4–44.9% | 134–136 µs | 14.9 | 15% | 1.25–1.27 ms | 3.5–11.1 ms | 20% |
| 4000 | `localWait` 500 µs | 2 | 18.0–20.2% | 120–126 µs | 12.4–12.6 | 13% | 1.29–1.32 ms | 2.9–4.1 ms | 56–59% |
| 4000 | `localWait` 1000 µs | 2 | 5.7–10.9% | 113–134 µs | 11.2–11.5 | 12–14% | 1.33–1.44 ms | 3.0–27.1 ms | 79–88% |
| 4000 | `never` | 2 | 0.0% | 112–118 µs | 10.6–10.7 | 11–12% | 1.33–1.39 ms | 3.3–11.2 ms |  |

With epoll the event loops use less than half of the CPU of NIO (113–156 µs per request) and the short waits don't cost extra:
each wait saves CPU in proportion to the steals it avoids (250 µs: 12% less than `shared` at 4000 requests/s, 500 µs: 20% less).
1000 µs steals the least (2–11%) and is as good as `never` at 2000 requests/s.

### Unit tests

`LocalWaitConnectionPoolTest` (20 tests, no database) runs the fork with fake connections, on real Vert.x event loops,
with timers that only fire when the test says so (`LocalWaitPoolHarness`, one for each Vert.x version, in
`src/testVertx4` and `src/testVertx5`). Run them with `gradle :hibernate-orm-benchmark-reactive-openloop:test`
(add `-Pstack=vertx5` for Vert.x 5). They check:

- a released connection goes to the oldest local waiter of its event loop first, never to a local waiter of another event loop,
  and stays free for the event loops that haven't waited long enough;
- a waiter whose wait is over takes a free connection of another event loop, or waits for the first release of any event loop,
  after the waiters that were there before, and a local waiter is served before them;
- a waiter is never served twice, even if its timer runs after it has been served, and a cancelled waiter isn't served;
- the queue depth shortcut (steal at once, or join the queue without a timer), and an event loop without connections not waiting;
- the creation affinity, with and without the round-robin;
- the eviction of the connections that aren't in use, a closed connection replaced for the oldest waiter,
  and a connection that fails to open (for a request, and for a waiter: the next waiter opens another);
- 25 random sequences of requests, releases and deadlines: a connection is never held by 2 requests,
  and once everything is released and the deadlines are over, no waiter is left.

### Conclusions

- **The policy works inside the Vert.x pool,** with the same results as `preferLocalWait`: almost no steals,
  nearly as little CPU as `never`, 8–24% less than `shared` where the event loops steal a lot, and no change otherwise.
  Idle eviction, maximum lifetime and failures of connections still work: the pool around the fork is the Vert.x one,
  and the fork has unit tests for the eviction, closed connections and connections that fail.
- **Use 1000 µs.** It's the shortest wait that works on every stack and in every setup of this section.
  Shorter waits (250–500 µs) only work when the local connection gets free quickly: otherwise a request waits
  and steals anyway, which cost up to 35% more CPU than `shared` on Vert.x 5 with NIO. 1500–2000 µs steal a little less
  at equal load; not measured with uneven load.
- **Where it hurts:** the median latency goes up by 0.1–0.2 ms at equal load, and by 0.17–0.3 ms for an event loop that has all the load
  and 1 connection. That's the price of not using the free connections of other event loops, `never` pays it too.
  With long sessions the wait is mostly wasted (it's `shared`, with the extra CPU if the wait is under 1 ms).
- **`localWaitDepth=1` looks worth having** for the event loop with all the load (median −0.15 ms at no cost),
  and changed nothing elsewhere. It's off by default until there is more data than 3 runs. **Creation affinity** changed nothing here.
- **For a change in Vert.x:** the fork is 215 lines longer than `SimpleConnectionPool` (429 added, 214 removed or changed),
  mostly the queues, `Acquire`, `Expire` and the statistics. Open questions:
  the wait adds latency, so it should be a `PoolOptions` setting that's off by default;
  the time-out of `getConnection` (`SqlConnectionPool#acquire` starts it in `onEnqueue`) now includes the local wait;
  the pool metrics (`enqueue`/`dequeue`) include it too; and the deadline timers need an event loop with a good timer resolution
  (they're late by up to 2 ms when the loop is busy). The pipelining (`concurrency` > 1) is handled but wasn't measured.
  The waiter queues and the timers cost nothing measurable here (`localWaitUs=0` is `shared`, and 1000 µs matches `never`).

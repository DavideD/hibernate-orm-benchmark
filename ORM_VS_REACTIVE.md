# Hibernate ORM or Hibernate Reactive: which one is faster?

The question: can Hibernate Reactive be faster than Hibernate ORM with blocking JDBC, and under which conditions?
"Faster" is measured three ways: the highest request rate that a system sustains with a good latency,
the CPU used per request, and the latency at a rate both can handle.

The answer is in section 9. In one sentence: with a thread pool and a connection pool of the same size, ORM and Reactive
sustain the same rate within about 10%, Reactive on epoll uses 12–23% less CPU per request when the CPUs are busy,
and Reactive wins by a wide margin only when the threads you can afford are far fewer than the connections.

## 1. What was compared

The same workload on both stacks, against the same PostgreSQL 18:

- A **session** is `openSession()`, `setDefaultReadOnly(true)`, 5 `find(World.class, id)` on random ids
  (a table of 10,000 rows with two `int` columns), `close()`. No transaction (auto-commit), no writes.
- With a **database delay**, the session starts with `select count(*) from (select pg_sleep(0.00x)) s`
  (a native query, one per session, not one per `find`). PostgreSQL sleeps and uses no CPU, the connection is busy.
  It stands for the network latency and the slower queries of a real database. 0, 1 and 5 ms were tested.
- The **pool** has the same number of connections: 8 or 32.
- **The ORM is the same version:** 7.4.13 on both sides. Hibernate Reactive 4.5.11 with Vert.x 5.1.10
  (the `vertx5` stack of `POOL_STEALING.md`, section 11), pgjdbc 42.7.7, Agroal 3.0.1 (`hibernate-agroal`).
- **The prepared statement caches are on for both:** `PgConnectOptions#setCachePreparedStatements(true)`,
  and the defaults of pgjdbc (a cache of 256 queries, server-side prepared statements after 5 executions).
- The **JVM** flags are the same: JDK 25, `-Xms1g -Xmx1g`, G1.

The PostgreSQL log shows the same statements for both: the `pg_sleep` query (if any), then 5 selects, as prepared statements,
with no `BEGIN` or `COMMIT` (auto-commit is on for the ORM too).

The ORM variants:

- **Platform threads:** a fixed pool of `threads` threads (`ThreadPoolExecutor`, unbounded queue). The default is `threads = poolSize`:
  a thread that has no connection can only wait for one, so more threads than connections add context switches
  and nothing else. Section 6 sweeps it.
- **Virtual threads:** `Thread.ofVirtual()`, one for each session.
- **Agroal** (what Quarkus uses, through `hibernate-agroal`) with `minSize = initialSize = maxSize = poolSize`.
  HikariCP would do the same: the pool of a blocking client only hands out connections.

The Reactive variants (the pools of `POOL_STEALING.md`): `shared` (the Quarkus-like pool), `localWait` (1000 µs, section 12 there)
and `perEventLoop`, each on NIO and on the native epoll transport (`nativeTransport=true`).
The number of event loops is the number of cores the JVM may use (section 8 has twice as many).
`PgConnectOptions#setPipeliningLimit` is in section 8.

## 2. The benchmark

`OrmVsReactiveBenchmark` (module `orm-vs-reactive`), started by `run_OrmVsReactive.sh` (see `--help`).
It uses the method of `POOL_STEALING.md` section 6: **open loop**. Sessions start at random times (Poisson arrivals)
at a fixed average rate, whatever the speed of the system, and the **latency is measured from the scheduled start**
(HdrHistogram), so a session that waits for a thread, an event loop or a connection counts as slow.

- Reactive: one `LoadVerticle` on each event loop starts its sessions itself, from a Vert.x timer (1 ms resolution).
- ORM: a scheduler thread submits each session to the executor at its scheduled time, with the same 1 ms granularity
  (it sleeps at least 1 ms, then submits all the sessions that are due). It is a cost of the blocking model:
  in a server, a thread of the pool runs the request that another thread received.

Every run reports the rate, p50 / p90 / p99 / p99.9 / max latency, errors, and:

- **CPU of the process per session:** `OperatingSystemMXBean#getProcessCpuTime`, all the threads (event loops, workers,
  Agroal, GC, JIT, scheduler).
- **Busy CPU per session:** the time the CPUs of the JVM were busy, from `/proc/stat`. It is more than the CPU of the process,
  because it includes the network stack of the kernel (soft interrupts), which the loopback traffic to the database makes significant
  (10–20 µs per session). It is the figure that says if the CPUs are the bottleneck.
- The CPU of PostgreSQL (the cgroup of its container), the number of platform threads, the time spent in GC,
  and the heap after a GC (fixed rate runs only).

The tables of sections 6 to 8 name the Reactive configurations like `CONFIGS` of the script: `reactive:<pool>[:epoll][:pipeN]`
(NIO unless `epoll`, `pipeN` is the pipelining limit).

**`mode=fixed`** runs at a rate for 20 s, after a 10 s warm-up.

**`mode=max`** looks for the highest rate that is **sustained**: in a 4 s step, at least 99% of the sessions that were scheduled
completed, the p99 latency is at most 10 ms (`sloMs`), and there are no errors. The rate starts near the
expected one and grows by a factor of 1.5 until a step fails, then bisects between the last step that passed and the first that failed,
until they are within 4%. The system drains between steps. A rate that fails is run once more, and fails only if it fails twice
(a GC or a noisy neighbour can fail a step). So:

- The resolution is about 4%, and the result is slightly optimistic. Differences below 5–8% aren't significant.
- The p99 limit of 10 ms stops most of the searches, not the throughput: the JVM CPUs are 56–99% busy at the end.
  Section 7 shows what a looser limit gives.

**CPUs.** The machine is a Ryzen 9 7950X3D (16 cores, SMT) in a virtual machine, with other tenants: the runs are noisy.
The JVM is pinned with `taskset` to 2 or 4 physical cores (CPUs 8–9 or 8–11, their SMT siblings are idle),
so that the CPUs of the JVM are the bottleneck. PostgreSQL runs in a container with host networking
(the port forwarding of a rootless container takes CPU outside the pinned CPUs), pinned to the CPUs 16–23
(the SMT siblings of idle cores) with a quota of 8 CPUs, and `max_connections=400`.
Its settings are the ones of `start_pg.sh` (`fsync=off`, ...), and the table is in memory: the database is fast.

**Is the database the bottleneck?** Not with 2 and 4 cores:

- The rate scales with the CPUs of the JVM: ORM with platform threads, 32 connections, no delay: 29.5–30.2k sessions/s with 2 cores,
  60.8k with 4, and 84.4–86.5k with 6 (section 8).
- PostgreSQL used 1.9 CPUs at the maximum with 2 JVM cores, 4.2 with 4 and 5.5 with 6, out of 8.
- The CPUs of the JVM were 97% busy at the maximum of the ORM with platform threads (1.93–1.94 cores of 2, 3.90 of 4).
  Reactive ends with 56–99% busy, because of the p99 limit.
- With a delay of 1 ms or more, the pool is the limit, not the CPU (section 4).

**The choices.** The event loops are as many as the cores (section 8), the platform threads as many as the connections (section 6),
and the pools have 8 connections (2 or 4 per event loop) or 32. Both are small enough to be the limit when the database is slow.

## 3. What is different from `POOL_STEALING.md`

The numbers of the two documents can't be compared:

- The stealing benchmark disabled the prepared statement cache of the Vert.x client (its default). Here it is enabled, like Quarkus
  does. It matters for Reactive: at 10,000 sessions/s with 2 cores and 32 connections, the CPU per session was 40–41 µs with the cache
  and 70–71 µs without (`shared` and `localWait`, epoll), while the ORM used 52–53 µs with the defaults of pgjdbc and
  54–55 µs with its cache disabled (`preparedStatementCacheQueries=0&prepareThreshold=0`; 3 runs each, `preparedStatements=false`).
  The Vert.x client prepares each statement in a round trip of its own when it doesn't cache. So **without the cache,
  Reactive would use about 30% more CPU than the ORM, with it about 20% less**.
- The sessions are read-only, and the steals aren't counted (`StealCounter` only forwards the calls: the benchmark needs it
  to replace the pool).
- The rates are much higher (the CPUs are the limit), and there are as many event loops as cores.

## 4. Maximum sustainable rate

The highest rate with a p99 under 10 ms. Ranges of 3 runs (2 runs with a delay of 1 ms), the configurations alternated
between rounds. Cells are in thousands of sessions per second.

#### DB latency 0 ms

**Max sustainable rate (thousand sessions/s)**

| Configuration | 2 cores, pool 8 | 2 cores, pool 32 | 4 cores, pool 8 | 4 cores, pool 32 |
|---|---|---|---|---|
| ORM, platform threads | 32.3–33.2 | 29.5–30.2 | 59.5 | 60.8 |
| ORM, virtual threads | 18.1–19.8 | 20.2–21.1 | 43.0–45.6 | 48.4–50.6 |
| Reactive `shared`, NIO | 29.1 | 33.0 | 38.8–39.7 | 59.1–60.8 |
| Reactive `shared`, epoll | 30.4 | 32.3–34.8 | 39.7 | 62.4–64.1 |
| Reactive `localWait`, NIO | 31.3–34.2 | 27.4–29.5 | 49.4–60.8 | 52.9–54.0 |
| Reactive `localWait`, epoll | 32.3–33.2 | 26.0–30.2 | 59.5–60.8 | 59.1–64.1 |
| Reactive `perEventLoop`, NIO | 19.8–27.2 | 21.1–22.5 | 62.6–66.4 | 38.2–45.0 |
| Reactive `perEventLoop`, epoll | 22.1–25.3 | 19.7–25.3 | 60.8–66.4 | 39.4–41.6 |

**CPU of the JVM CPUs per session at that rate (µs, busy)**

| Configuration | 2 cores, pool 8 | 2 cores, pool 32 | 4 cores, pool 8 | 4 cores, pool 32 |
|---|---|---|---|---|
| ORM, platform threads | 60 | 64–66 | 55 | 64 |
| ORM, virtual threads | 59–61 | 48–49 | 72–73 | 55 |
| Reactive `shared`, NIO | 63–64 | 59–60 | 82–85 | 63–66 |
| Reactive `shared`, epoll | 55 | 54 | 63 | 57–58 |
| Reactive `localWait`, NIO | 58–61 | 64–67 | 63–71 | 68–69 |
| Reactive `localWait`, epoll | 52–53 | 54–56 | 56–57 | 55–56 |
| Reactive `perEventLoop`, NIO | 66–72 | 71–72 | 58–61 | 70–71 |
| Reactive `perEventLoop`, epoll | 54–55 | 56–58 | 54–55 | 57–59 |

#### DB latency 1 ms

**Max sustainable rate (thousand sessions/s)**

| Configuration | 2 cores, pool 8 | 2 cores, pool 32 | 4 cores, pool 8 | 4 cores, pool 32 |
|---|---|---|---|---|
| ORM, platform threads | 3.4 | 14.1 | 3.4 | 14.4 |
| ORM, virtual threads | 3.1 | 13.0 | 3.1 | 13.7 |
| Reactive `shared`, NIO | 3.0–3.3 | 13.7 | 3.2–3.3 | 13.7–14.1 |
| Reactive `shared`, epoll | 3.2–3.3 | 13.4–13.7 | 3.2–3.4 | 13.7 |
| Reactive `localWait`, NIO | 3.0 | 13.0–13.4 | 3.0 | 13.4 |
| Reactive `localWait`, epoll | 3.0–3.1 | 13.0–13.4 | 3.1–3.2 | 13.4–13.7 |
| Reactive `perEventLoop`, NIO | 3.1 | 13.4–13.7 | 2.5–2.6 | 13.0 |
| Reactive `perEventLoop`, epoll | 2.7–3.0 | 13.4–13.7 | 2.6 | 13.0 |

**CPU of the JVM CPUs per session at that rate (µs, busy)**

| Configuration | 2 cores, pool 8 | 2 cores, pool 32 | 4 cores, pool 8 | 4 cores, pool 32 |
|---|---|---|---|---|
| ORM, platform threads | 61 | 63–66 | 68–98 | 53–56 |
| ORM, virtual threads | 129 | 91–92 | 148–150 | 113–116 |
| Reactive `shared`, NIO | 178–185 | 111–112 | 242–252 | 159–169 |
| Reactive `shared`, epoll | 90–92 | 79–81 | 100–101 | 89 |
| Reactive `localWait`, NIO | 129–134 | 104 | 174–179 | 136–139 |
| Reactive `localWait`, epoll | 87 | 76–79 | 95–97 | 80–84 |
| Reactive `perEventLoop`, NIO | 107–143 | 97–99 | 112–113 | 101–104 |
| Reactive `perEventLoop`, epoll | 100 | 74–76 | 92–95 | 80–81 |

#### DB latency 5 ms

**Max sustainable rate (thousand sessions/s)**

| Configuration | 2 cores, pool 8 | 2 cores, pool 32 | 4 cores, pool 8 | 4 cores, pool 32 |
|---|---|---|---|---|
| ORM, platform threads | 1.2 | 5.5–5.8 | 1.2 | 5.8 |
| ORM, virtual threads | 1.0–1.1 | 5.2–5.3 | 1.0 | 5.3 |
| Reactive `shared`, NIO | 1.1 | 4.4–5.6 | 1.1 | 5.5–5.6 |
| Reactive `shared`, epoll | 1.1–1.2 | 4.4–5.6 | 1.1 | 5.6 |
| Reactive `localWait`, NIO | 1.0 | 5.1–5.2 | 0.9–1.0 | 5.2 |
| Reactive `localWait`, epoll | 1.0 | 5.2–5.3 | 1.0 | 5.2–5.3 |
| Reactive `perEventLoop`, NIO | 0.8 | 5.1–5.2 | 0.4 | 4.4–4.5 |
| Reactive `perEventLoop`, epoll | 0.8 | 4.4–5.1 | 0.4 | 4.4–4.5 |

**CPU of the JVM CPUs per session at that rate (µs, busy)**

| Configuration | 2 cores, pool 8 | 2 cores, pool 32 | 4 cores, pool 8 | 4 cores, pool 32 |
|---|---|---|---|---|
| ORM, platform threads | 69–82 | 74–78 | 90–110 | 66–96 |
| ORM, virtual threads | 143–146 | 90–94 | 159–174 | 105–111 |
| Reactive `shared`, NIO | 164–175 | 112–132 | 204–221 | 183–202 |
| Reactive `shared`, epoll | 101–126 | 82–88 | 116–162 | 94–100 |
| Reactive `localWait`, NIO | 148–165 | 106–110 | 193–202 | 145–174 |
| Reactive `localWait`, epoll | 106–264 | 83–84 | 116–134 | 85–91 |
| Reactive `perEventLoop`, NIO | 126–174 | 96–98 | 199–307 | 116–120 |
| Reactive `perEventLoop`, epoll | 106–118 | 81–82 | 193–235 | 84–103 |

What the numbers show:

- **With no delay the CPUs are the limit, and the ORM with platform threads is as fast as the best Reactive configuration.**
  2 cores: 32.3–33.2k (8 connections) and 29.5–30.2k (32) for the ORM, against 31.3–34.2k and 32.3–34.8k for the best Reactive
  configurations. 4 cores: 59.5k and 60.8k, against 60.8–66.4k and 62.4–64.1k. The ranges overlap, and some Reactive
  points are 5–12% higher.
- **Reactive uses less CPU per session when the CPUs are busy, only on epoll.** At the maximum, the busy CPU per session
  is 52–59 µs for the Reactive pools on epoll (63 µs for `shared` with 8 connections and 4 cores), 58–72 µs on NIO (up to 85),
  and 55–66 µs for the ORM with platform threads: about 13% less for epoll.
  It doesn't give a 13% higher rate: Reactive on epoll typically stops at 83–88% busy (63–94% in all the runs) because of the p99 limit (section 7).
- **Virtual threads are slower:** 18–21k with 2 cores, 43–51k with 4 (55–85% of the platform threads). They use 1.0–1.2 busy cores
  of 2 at their maximum: the CPUs aren't the limit, the latency of a session goes over 10 ms first.
  I didn't find why. The tests with `-Djdk.readPollers=2` and `4` didn't change it (20.3–20.8k with 2 cores, 48.4–51.8k with 4;
  one run each), and `-Djdk.virtualThreadScheduler.parallelism=4` on 2 cores gave 27.0k (one run).
- **The pool matters a lot for Reactive.** With 8 connections and 4 cores, `shared` stops at 38.8–39.7k (an event loop has 2 connections,
  and steals are expensive), `localWait` reaches 49.4–60.8k and `perEventLoop` 62.6–66.4k.
  With 32 connections, `shared` and `localWait` are at 53–64k, but `perEventLoop` falls to 38–45k.
  I didn't find why: at 45,000 sessions/s with 4 cores its p99 was 16–23 ms with 32 connections and 1.8–2.1 ms with 8,
  and the other pools were at 2–3 ms (with a stall of 0.5 s in one of two runs of `localWait` and of the ORM).
- **With a delay of 1 or 5 ms, the pool is the limit and the stack matters little.** With 32 connections: 13.0–14.4k sessions/s
  at 1 ms (a session holds a connection for about 2.3 ms) and 4.4–5.8k at 5 ms; with 8 connections: 2.5–3.4k and 0.4–1.2k.
  The rate is about the number of connections divided by the time a session holds one.
  The ORM with platform threads is the highest in most of these cells, by 0–10%.
  At 5 ms the limit of 10 ms for the p99 is only twice the delay, so it compares the queueing more than the throughput:
  `perEventLoop` with 4 event loops and 2 connections each reaches 0.4k, against 1.0–1.2k for the pools shared by all.
- **The CPU per session depends on the load.** In the runs with a delay the CPUs are idle most of the time (0.1–1 busy cores),
  each wake-up of a thread or an event loop is a larger part of the cost, and the ORM with platform threads is the cheapest
  (53–110 µs busy per session): Reactive on epoll costs 10–65% more, Reactive on NIO and virtual threads 40–200% more.
  Section 5 compares them at the same rate.

## 5. The same rate for everyone

50% of the lowest maximum found in the regime (section 4), 3 runs each, so that all the configurations do the same work. "CPU" is the process, "busy" the CPUs of the JVM.

#### 2 cores, pool 8, DB latency 0 ms, 9000 sessions/s

| Configuration | Runs | Process CPU/session (µs) | JVM CPUs busy/session (µs) | Busy cores | p50 (ms) | p99 (ms) | p99.9 (ms) | Threads | Heap after GC (MB) |
|---|---|---|---|---|---|---|---|---|---|
| ORM, platform threads | 3 | 50 | 70 | 0.62 | 0.82 | 1.3 | 3.1–4.7 | 17 | 11–12 |
| ORM, virtual threads | 3 | 48–51 | 63–67 | 0.57–0.60 | 0.85–0.90 | 1.7–996.1 | 4.1–1306.5 | 14 | 12 |
| Reactive `shared`, NIO | 3 | 62–63 | 81–84 | 0.73–0.76 | 1.00–1.01 | 2.9–34.6 | 17.1–60.0 | 13 | 15–16 |
| Reactive `shared`, epoll | 3 | 45–46 | 62–64 | 0.56–0.57 | 0.79 | 1.3 | 6.1–8.5 | 13 | 15–16 |
| Reactive `localWait`, NIO | 3 | 50–52 | 70–73 | 0.63–0.66 | 0.97 | 1.7 | 6.2–13.4 | 13 | 15–16 |
| Reactive `localWait`, epoll | 3 | 42–43 | 61–62 | 0.55–0.56 | 0.79–0.80 | 1.3 | 6.4–7.7 | 13 | 15 |
| Reactive `perEventLoop`, NIO | 3 | 49–51 | 69 | 0.62 | 0.96–0.97 | 1.7 | 5.2–14.5 | 13 | 15 |
| Reactive `perEventLoop`, epoll | 3 | 42–43 | 62–65 | 0.56–0.59 | 0.80 | 1.3 | 6.6–13.9 | 13 | 15 |

#### 2 cores, pool 8, DB latency 1 ms, 1500 sessions/s

| Configuration | Runs | Process CPU/session (µs) | JVM CPUs busy/session (µs) | Busy cores | p50 (ms) | p99 (ms) | p99.9 (ms) | Threads | Heap after GC (MB) |
|---|---|---|---|---|---|---|---|---|---|
| ORM, platform threads | 3 | 71–72 | 93–109 | 0.14–0.16 | 2.79–2.80 | 3.4 | 4.4–4.5 | 17 | 12–13 |
| ORM, virtual threads | 3 | 120–125 | 137–145 | 0.21–0.22 | 2.83–2.84 | 3.5 | 4.9–5.8 | 14 | 12 |
| Reactive `shared`, NIO | 3 | 98–101 | 117–120 | 0.18 | 2.82–2.83 | 3.5–3.6 | 6.6–8.8 | 13 | 15 |
| Reactive `shared`, epoll | 3 | 79–80 | 97–109 | 0.14–0.16 | 2.80 | 3.4–3.6 | 7.3–12.7 | 13 | 15 |
| Reactive `localWait`, NIO | 3 | 89–90 | 108–116 | 0.16–0.17 | 2.85 | 4.0–4.1 | 6.3–10.3 | 13 | 15–16 |
| Reactive `localWait`, epoll | 3 | 78–79 | 97–122 | 0.14–0.18 | 2.83 | 4.0 | 8.1–11.5 | 13 | 15 |
| Reactive `perEventLoop`, NIO | 3 | 80–83 | 98–105 | 0.15–0.16 | 2.85 | 4.2–4.3 | 7.3–8.7 | 13 | 15 |
| Reactive `perEventLoop`, epoll | 3 | 73–74 | 96–101 | 0.14–0.15 | 2.83 | 4.3–4.4 | 8.9–13.2 | 13 | 15–16 |

#### 2 cores, pool 32, DB latency 0 ms, 10000 sessions/s

| Configuration | Runs | Process CPU/session (µs) | JVM CPUs busy/session (µs) | Busy cores | p50 (ms) | p99 (ms) | p99.9 (ms) | Threads | Heap after GC (MB) |
|---|---|---|---|---|---|---|---|---|---|
| ORM, platform threads | 3 | 52–53 | 71–72 | 0.70–0.71 | 0.87–0.88 | 1.4–2.6 | 3.8–5.7 | 41 | 13–17 |
| ORM, virtual threads | 3 | 39 | 54–55 | 0.54–0.55 | 0.91–0.92 | 1.7 | 4.1–5.1 | 14 | 13–14 |
| Reactive `shared`, NIO | 3 | 49–50 | 69–74 | 0.69–0.74 | 1.06–1.07 | 2.1–4.2 | 8.7–22.1 | 13 | 18–21 |
| Reactive `shared`, epoll | 3 | 40–41 | 60–61 | 0.60–0.61 | 0.84–0.85 | 1.5 | 4.3–10.3 | 13 | 15 |
| Reactive `localWait`, NIO | 3 | 46–49 | 66–70 | 0.66–0.70 | 1.05 | 2.1 | 6.8–9.0 | 13 | 15 |
| Reactive `localWait`, epoll | 3 | 40–41 | 60–62 | 0.60–0.62 | 0.84–0.85 | 1.5 | 6.1–10.2 | 13 | 15–18 |
| Reactive `perEventLoop`, NIO | 3 | 48–50 | 69–79 | 0.69–0.79 | 1.06–1.07 | 2.1–2.3 | 8.8–11.9 | 13 | 15 |
| Reactive `perEventLoop`, epoll | 3 | 41 | 60–62 | 0.60–0.62 | 0.84–0.85 | 1.5 | 7.0–9.8 | 13 | 15 |

#### 2 cores, pool 32, DB latency 1 ms, 6500 sessions/s

| Configuration | Runs | Process CPU/session (µs) | JVM CPUs busy/session (µs) | Busy cores | p50 (ms) | p99 (ms) | p99.9 (ms) | Threads | Heap after GC (MB) |
|---|---|---|---|---|---|---|---|---|---|
| ORM, platform threads | 3 | 63–64 | 88–89 | 0.57–0.58 | 2.94–2.95 | 3.5–4.0 | 7.1–9.4 | 41 | 13–17 |
| ORM, virtual threads | 3 | 59–60 | 81–82 | 0.52–0.53 | 2.95–2.97 | 3.8 | 8.2–9.7 | 14 | 13 |
| Reactive `shared`, NIO | 3 | 63–64 | 88–89 | 0.57–0.58 | 2.89 | 3.7 | 8.7–9.7 | 13 | 15 |
| Reactive `shared`, epoll | 3 | 56–58 | 80–83 | 0.52–0.54 | 2.87–2.88 | 3.5 | 9.0–11.8 | 13 | 15–16 |
| Reactive `localWait`, NIO | 3 | 62 | 86 | 0.56 | 2.89–2.90 | 3.6–3.7 | 9.3–14.5 | 13 | 15–17 |
| Reactive `localWait`, epoll | 3 | 56–57 | 81–82 | 0.52–0.53 | 2.88 | 3.5 | 9.4–13.3 | 13 | 15–17 |
| Reactive `perEventLoop`, NIO | 3 | 60–61 | 84–87 | 0.54–0.56 | 2.89 | 3.7–4.1 | 9.8–15.1 | 13 | 15 |
| Reactive `perEventLoop`, epoll | 3 | 56–58 | 82–84 | 0.53–0.54 | 2.88 | 3.5 | 8.6–12.0 | 13 | 15–17 |

#### 4 cores, pool 32, DB latency 0 ms, 19000 sessions/s

| Configuration | Runs | Process CPU/session (µs) | JVM CPUs busy/session (µs) | Busy cores | p50 (ms) | p99 (ms) | p99.9 (ms) | Threads | Heap after GC (MB) |
|---|---|---|---|---|---|---|---|---|---|
| ORM, platform threads | 3 | 52 | 73–74 | 1.39–1.40 | 0.85–0.86 | 1.3 | 3.1–3.6 | 41 | 13–14 |
| ORM, virtual threads | 3 | 44 | 70 | 1.32 | 0.82–0.83 | 1.4 | 3.3–4.2 | 16 | 13–14 |
| Reactive `shared`, NIO | 3 | 59–60 | 79–86 | 1.50–1.64 | 0.98–0.99 | 1.9 | 3.2–3.6 | 15 | 16–17 |
| Reactive `shared`, epoll | 3 | 42–43 | 64–65 | 1.22–1.23 | 0.84 | 1.4 | 2.5–3.1 | 15 | 16–18 |
| Reactive `localWait`, NIO | 3 | 48–49 | 68–70 | 1.30–1.33 | 1.03 | 1.8–1.9 | 2.5–4.1 | 15 | 17 |
| Reactive `localWait`, epoll | 3 | 41 | 61–63 | 1.16–1.19 | 0.84 | 1.4 | 2.2–2.7 | 15 | 16–17 |
| Reactive `perEventLoop`, NIO | 3 | 48–49 | 69–70 | 1.31–1.32 | 1.03 | 1.9 | 3.0–3.4 | 15 | 16–17 |
| Reactive `perEventLoop`, epoll | 3 | 40–41 | 60–62 | 1.14–1.17 | 0.83–0.84 | 1.4 | 2.0–3.4 | 15 | 16–17 |

#### 4 cores, pool 32, DB latency 5 ms, 2200 sessions/s

| Configuration | Runs | Process CPU/session (µs) | JVM CPUs busy/session (µs) | Busy cores | p50 (ms) | p99 (ms) | p99.9 (ms) | Threads | Heap after GC (MB) |
|---|---|---|---|---|---|---|---|---|---|
| ORM, platform threads | 3 | 66–69 | 81–90 | 0.18–0.20 | 5.77 | 6.3 | 6.7–7.5 | 41 | 13 |
| ORM, virtual threads | 3 | 123–124 | 119–134 | 0.26–0.29 | 5.80–5.81 | 6.3 | 6.7–7.8 | 16 | 14 |
| Reactive `shared`, NIO | 3 | 100–103 | 125–166 | 0.28–0.36 | 5.78–5.79 | 6.4 | 8.8–9.3 | 15 | 16–17 |
| Reactive `shared`, epoll | 3 | 74–76 | 96–104 | 0.21–0.23 | 5.78 | 6.3 | 8.7–9.1 | 15 | 16–18 |
| Reactive `localWait`, NIO | 3 | 100–101 | 127–130 | 0.28–0.29 | 5.79 | 6.4 | 8.9–9.7 | 15 | 16–17 |
| Reactive `localWait`, epoll | 3 | 74 | 96–119 | 0.21–0.26 | 5.78–5.79 | 6.3 | 8.7–9.0 | 15 | 16–17 |
| Reactive `perEventLoop`, NIO | 3 | 99–100 | 126–138 | 0.28–0.30 | 5.79 | 6.4–6.5 | 8.9–9.9 | 15 | 16–17 |
| Reactive `perEventLoop`, epoll | 3 | 74–78 | 94–107 | 0.21–0.24 | 5.78–5.79 | 6.3–6.4 | 8.6–9.6 | 15 | 16–17 |

What the numbers show:

- **Without a delay, Reactive on epoll uses 12–23% less CPU than the ORM with platform threads:** 40–46 µs per session against 50–53
  (process), 60–65 against 70–74 (busy). On NIO Reactive uses 46–63 µs: from 5% less to 24% more than the ORM.
  The three pools cost the same within ±5%, except `shared` on NIO with 8 connections and 2 cores (62–63 µs).
- **With a delay it depends on the rate:** at 1 ms and 6500 sessions/s Reactive on epoll uses 56–58 µs against 63–64 for the ORM,
  and the NIO pools 60–64. At 1 ms and 1500/s the ORM uses 71–72 µs, Reactive on epoll 73–80 and on NIO 80–101.
  At 5 ms and 2200/s: 66–69 µs, 74–78 and 99–103. The more idle the system, the more the wake-ups count.
- **Virtual threads are among the cheapest without a delay** (39–51 µs), **and the most expensive at low rates with a delay:**
  120–125 µs at 1 ms and 1500/s, 123–124 µs at 5 ms and 2200/s, against 66–72 µs for platform threads, but 59–60 µs at 1 ms and 6500/s.
  One of the 3 runs of virtual threads at 9000 sessions/s (2 cores, 8 connections, no delay) had a p99 of 996 ms: a stall
  that platform threads and Reactive didn't have in these runs.
- **The latency is similar at p50 and p99, and Reactive has a longer tail with 2 cores.** Without a delay the p50 is 0.79–0.88 ms for the
  ORM and Reactive on epoll, 0.96–1.07 ms on NIO; the p99 is 1.3–1.4 ms for the ORM (2.6 ms once), 1.3–1.5 ms on epoll, 1.7–4.2 ms on NIO
  (34.6 ms once). The p99.9 with 2 cores is 3.1–5.7 ms for the ORM against 4.3–13.9 ms for Reactive on epoll; with 4 cores it is
  3.1–3.6 ms against 2.0–3.4 ms. With a delay the p50 and p99 are the same for everyone, within 1 ms.
- **Threads and heap:** 41 platform threads for the ORM with 32 connections, 13–16 for Reactive and virtual threads (event loops,
  pool, GC, JIT, carriers; the virtual threads aren't counted). The heap after a GC is 11–21 MB for all: the data is small,
  it isn't a difference.

## 6. How many threads does the ORM need?

The platform threads, swept with 32 connections. Maximum rate in thousands of sessions/s, 2 runs each:

| Configuration | 2 cores, pool 32, 5 ms | 2 cores, pool 32, 1 ms | 2 cores, pool 32, 0 ms | 4 cores, pool 32, 5 ms |
|---|---|---|---|---|
| ORM, `threads=4` | 0.4 | 1.6 | 26.4–31.2 | 0.4 |
| ORM, `threads=8` | 1.2 | 3.3–3.4 | 32.1–32.9 | 1.2 |
| ORM, `threads=16` | 2.6 | 7.0 | 31.2 | 2.6 |
| ORM, `threads=32` | 5.5–5.6 | 13.9–14.2 | 29.5–30.4 | 5.7 |
| ORM, `threads=64` | 5.1 | 12.0–12.7 | 20.8 | 5.3 |
| ORM, virtual threads | 5.2–5.3 | 13.3 | 20.2–21.4 | 5.2–5.5 |
| `reactive:localWait:epoll` | 5.2 | 13.0 | 26.4–27.0 | 5.2 |

and the busy CPU per session in µs at that maximum:

| Configuration | 2 cores, pool 32, 5 ms | 2 cores, pool 32, 1 ms | 2 cores, pool 32, 0 ms | 4 cores, pool 32, 5 ms |
|---|---|---|---|---|
| ORM, `threads=4` | 104–111 | 69–71 | 55–56 | 160–612 |
| ORM, `threads=8` | 77–148 | 61–65 | 59–60 | 94–95 |
| ORM, `threads=16` | 63–67 | 60–61 | 62–63 | 75 |
| ORM, `threads=32` | 77–80 | 60–67 | 64–65 | 65–67 |
| ORM, `threads=64` | 83–87 | 95–97 | 67–68 | 68–95 |
| ORM, virtual threads | 87–92 | 94–95 | 49 | 106–107 |
| `reactive:localWait:epoll` | 80–82 | 76–77 | 56 | 87–119 |

What the numbers show:

- **With a delay, the rate is `threads / time of a session`:** 4 threads give 0.4k sessions/s at 5 ms, 8 give 1.2k, 16 give 2.6k,
  32 give 5.5–5.7k (with 2 and 4 cores). A thread holds a connection for the whole session, including the time the database takes. To use 32 connections,
  the ORM needs 32 threads. 64 threads are no better (5.1–5.3k): the 32 extra ones wait for a connection.
- **Reactive and virtual threads don't have this limit:** 2 or 4 event loops run 32 sessions at a time (5.2k).
  This is where Reactive wins the most: with 4 threads, 5.2k against 0.4k. But 32 platform threads reach the same 5.5k:
  the limit is the number of threads you can afford, and 32 is cheap.
- **Without a delay, fewer threads are better:** 8 threads give 32.1–32.9k with 2 cores, against 29.5–30.4k for 32 and 20.8k for 64.
  4 threads (26.4–31.2k) used 55–56 µs of busy CPU per session against 64–65 µs with 32 threads and 67–68 µs with 64:
  the context switches of the extra threads. A good thread count is the number of connections when the database is slow,
  and about the number of cores when the CPUs are the limit. Section 4 uses the number of connections: with 32 connections and 2 cores,
  the ORM numbers could be up to 10% higher.

## 7. A looser limit on the latency

The same search with a p99 limit of 50 ms (`sloMs=50`), no delay, 2 runs each. Rate in thousands of sessions/s:

| Configuration | 2 cores, pool 8, 0 ms | 4 cores, pool 32, 0 ms |
|---|---|---|
| ORM, platform threads | 32.8 | 61.9 |
| `reactive:shared:epoll` | 30.9 | 63.8 |
| `reactive:localWait` | 34.7–35.6 | 63.8–65.6 |
| `reactive:localWait:epoll` | 36.6 | 65.6–67.5 |

and the p99 at that rate in ms:

| Configuration | 2 cores, pool 8, 0 ms | 4 cores, pool 32, 0 ms |
|---|---|---|
| ORM, platform threads | 11.1–14.5 | 13.6–29.6 |
| `reactive:shared:epoll` | 16.1–37.5 | 8.8–26.0 |
| `reactive:localWait` | 26.7–43.4 | 34.3–38.8 |
| `reactive:localWait:epoll` | 29.3–42.3 | 21.1–27.7 |

What the numbers show:

- With a looser limit Reactive gets ahead: `localWait` on epoll reaches 36.6k against 32.8k for the ORM with 2 cores and 8 connections (+11%),
  and 65.6–67.5k against 61.9k with 4 cores and 32 connections (+6–9%). `shared` on epoll is at 30.9k and 63.8k (−6% and +3%).
- It is the lower CPU per session (section 5) that shows, once the queueing is allowed. The p99 at that rate is not clearly worse:
  9–43 ms for Reactive, 11–30 ms for the ORM.
- At 10 ms Reactive on epoll typically stops at 83–88% busy and the ORM at 97%, so the ORM keeps its p99 low longer. A likely reason: the sessions
  of the ORM share the CPUs through the scheduler of the kernel (a slow session doesn't hold up the others), while an event loop does
  the work of its sessions one step after the other. This was not tested.

## 8. Other variants

**Pipelining.** `PgConnectOptions#setPipeliningLimit` has no effect: 1, 16 and the default (256 in Vert.x 5.1) gave the same
maximum rate and CPU per session, 2 runs each, `shared` and `localWait` on epoll. Maximum rate in thousands of sessions/s:

| Configuration | 2 cores, pool 8, 0 ms | 4 cores, pool 32, 0 ms | 2 cores, pool 8, 1 ms |
|---|---|---|---|
| `reactive:shared:epoll` | 30.4 | 60.8–64.1 | 3.2–3.3 |
| `reactive:shared:epoll:pipe1` | 30.4 | 62.4 | 3.3 |
| `reactive:shared:epoll:pipe16` | 30.4 | 60.8–64.1 | 3.3 |
| `reactive:localWait:epoll` | 32.1–32.9 | 62.4–64.1 | 3.0 |
| `reactive:localWait:epoll:pipe1` | 32.9–33.8 | 52.9–64.1 | 3.0–3.1 |
| `reactive:localWait:epoll:pipe16` | 33.8–34.6 | 59.1 | 3.0–3.1 |

and the busy CPU per session in µs:

| Configuration | 2 cores, pool 8, 0 ms | 4 cores, pool 32, 0 ms | 2 cores, pool 8, 1 ms |
|---|---|---|---|
| `reactive:shared:epoll` | 55–56 | 58 | 90–91 |
| `reactive:shared:epoll:pipe1` | 54–55 | 58 | 89–93 |
| `reactive:shared:epoll:pipe16` | 55–56 | 57–59 | 91–94 |
| `reactive:localWait:epoll` | 52–53 | 56 | 92–117 |
| `reactive:localWait:epoll:pipe1` | 53 | 56–57 | 85–90 |
| `reactive:localWait:epoll:pipe16` | 53 | 56–57 | 89 |

Pipelining sends the next command of a connection without waiting for the response to the previous one. It only helps when several
commands wait for the same connection. Hibernate Reactive keeps the connection for the whole session and runs its statements one after the
other, so there is probably never a second command to send.

**Event loops.** With twice as many event loops as cores (epoll, no delay, 32 connections; 2 runs each):

| Configuration | 2 cores, pool 32, 0 ms | 4 cores, pool 32, 0 ms |
|---|---|---|
| `reactive:shared:epoll` | 30.9 | 48.8–51.2 |
| `reactive:localWait:epoll` | 30.0–32.8 | 53.8–60.0 |
| `reactive:perEventLoop:epoll` | 26.9–27.5 | 52.5–55.0 |

Against 32.3–34.8k, 26.0–30.2k and 19.7–25.3k with as many event loops as cores on 2 cores, and 62.4–64.1k, 59.1–64.1k and
39.4–41.6k on 4 cores (section 4). More event loops cost `shared` 4–24%, give `perEventLoop` 6–40% more, and `localWait` between −9% and +26% (within the noise of its
ranges). As many event loops as cores is right for the shared pools; `perEventLoop` likes more partitions.

**More cores.** To check the database, 6 cores (CPUs 8–13), no delay, 2 runs each, in thousands of sessions/s:

| Configuration | 6 cores, pool 32, 0 ms | 6 cores, pool 64, 0 ms |
|---|---|---|
| ORM, platform threads | 84.4–86.5 | 82.3–84.4 |
| `reactive:shared:epoll` | 75.9–78.0 | 82.3–90.7 |
| `reactive:localWait:epoll` | 84.4 | 82.3–86.5 |

Both stacks scale from 2 to 4 to 6 cores (14–15k sessions/s per core). Reactive ends at 4.9–5.7 busy cores of 6, the ORM at 5.7–5.9.
The virtual threads ran out of memory (1 GB of heap) in the overload steps in all the 4 runs and are not in the table: the benchmark never
rejects a request, so a system that can't keep up piles up virtual threads (and tasks, for the other variants). A server would limit the queue.

## 9. Conclusions

Hibernate Reactive was **faster than Hibernate ORM** in these cases:

1. **CPU per session when the CPUs are busy and the database is fast:** 12–23% less than platform threads on the epoll transport,
   with the prepared statement cache. On NIO it is 5% less to 24% more. Without the cache it is 30% more.
   With a loose limit on the latency (p99 under 50 ms) it also sustains 6–11% more sessions per second.
2. **When the ORM has fewer threads than connections:** 4 or 8 threads against 32 connections and a database that takes milliseconds:
   Reactive sustains 5.2k sessions/s, the ORM 0.4k or 1.2k. This is the case of a small thread pool, a very large connection pool,
   or sessions that wait for something other than the database. Virtual threads solve it too (5.2–5.3k).
3. **Against virtual threads when the load is low and the database slow:** 73–80 µs of CPU per session on epoll against 120–125 µs.

Hibernate ORM was **faster or equal** in these cases:

1. **Whenever it can have as many threads as connections** (32 here): the same maximum rate as the best Reactive configuration (±10%),
   a lower tail with 2 cores (p99.9 3.1–5.7 ms against 4.3–13.9 ms at half load), and, with a database that takes 1 ms or more and a load
   well under the maximum, the lowest CPU per session (up to 12% less than Reactive on epoll, up to 35% less than on NIO).
2. **Against a poor Reactive setup**, which costs more than the choice of the stack: NIO instead of epoll (10–40% more CPU), no prepared statement
   cache (+70%), `perEventLoop` with 32 connections (38–45k against 60k), `shared` with 2 connections per event loop (39k against 49–66k).
3. **Against virtual threads at high load**, which sustain 55–85% of the rate of platform threads under a p99 of 10 ms (JDK 25, not tuned).

They are **the same** for the maximum rate when the database is slow (the pool is the limit: 13.0–14.4k sessions/s at 1 ms,
4.4–5.8k at 5 ms, 32 connections), for the p50 latency, the heap, and for pipelining (no effect).

In short: **Hibernate Reactive doesn't make this workload faster. It makes it cheaper by 12–23% of the CPU when the CPUs are busy
(epoll only), and lets a few threads serve many connections. Blocking JDBC with a thread for each connection sustains the same rate with
a better tail.** The reasons to choose Reactive are the thread count and a non-blocking stack from end to end, not throughput.

## 10. Caveats

- One machine, one workload: 5 primary key reads, no transaction, no writes, no lazy loading, 10,000 rows, loopback, a database with
  `fsync=off`. A more complex workload spends more CPU in Hibernate itself (the same code on both stacks), which brings the two closer.
- The delay is `pg_sleep`: the connection is busy and the database uses no CPU. A real remote database also costs the network stack
  and wake-ups on the client. Only the loopback is in the busy CPU here.
- A virtual machine with other tenants: the runs are noisy. Repetitions of a configuration differ by up to 10–20% in the maximum rate
  (the resolution of the search is 4%), and a few single runs had stalls of 0.5–1 s that no configuration is immune to.
  The CPU states of the idle cores add latency to a wake-up, which the benchmark can't remove.
- The maximum rate depends on the limit of 10 ms for the p99 (section 7), on the 4 s steps, and on the retry of a failed step,
  which makes it slightly optimistic for all.
- The requests start differently: the ORM has a scheduler thread and a hand-off to a worker, Reactive starts on the event loop.
  A server with blocking or with reactive endpoints works that way, but the cost of an HTTP server, Mutiny or Quarkus is not included.
- JDK 25 virtual threads (no pinning with `synchronized`, JEP 491), pgjdbc 42.7.7 and Agroal 3.0.1. Other versions, or JDK 21, might differ.
  The scheduler of the virtual threads wasn't tuned beyond the three tests of section 4.
- The pools of 8 and 32 connections are small. With hundreds of connections the ORM needs hundreds of threads (their stacks aren't
  measured here) and Reactive doesn't.
- `localWait` keeps statistics (`LocalWaitStats`) that the other pools don't. The cost is small and wasn't measured.
- Not tested: HikariCP, writes and transactions, the Quarkus integration, ORM 6.6, Vert.x 4, bigger sessions, more than 6 cores,
  a database on another machine.

## 11. Reproducing

```
./run_OrmVsReactive.sh                  # max rate, 4 cores, 8 connections, no delay, the ORM and the 3 Reactive pools on NIO
CPUS="2 4" POOL_SIZES="8 32" LATENCIES="0 1 5" REPEAT=3 \
  CONFIGS="orm:platform orm:virtual reactive:shared reactive:shared:epoll reactive:localWait reactive:localWait:epoll" \
  ./run_OrmVsReactive.sh                # section 4 (hours)
MODE=fixed RATE=10000 CPUS=2 POOL_SIZES=32 REPEAT=3 ./run_OrmVsReactive.sh              # section 5
CONFIGS="orm:platform:4 orm:platform:8 orm:platform" LATENCIES=5 POOL_SIZES=32 ./run_OrmVsReactive.sh   # section 6
./run_OrmVsReactive.sh sloMs=50         # section 7
CONFIGS="reactive:shared:epoll reactive:shared:epoll:pipe1" ./run_OrmVsReactive.sh      # section 8
```

Gradle 8.4 of the wrapper can't run on JDK 25: the runs used `GRADLE=gradle` (9.4.1) with `JAVA_HOME=/usr/lib/jvm/java-25-openjdk`.
The raw data isn't in git.

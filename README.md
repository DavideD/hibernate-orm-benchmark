## Connection stealing benchmarks

The benchmarks about connection stealing in the Vert.x SQL client pool and the CPU use of Vert.x 5,
with their results:

- [`POOL_STEALING.md`](POOL_STEALING.md): connection stealing, the open loop benchmark (`reactive-openloop`, `run_OpenLoopSteal.sh`),
  the steal policies and the `localWait` pool
- [`ORM_VS_REACTIVE.md`](ORM_VS_REACTIVE.md): Hibernate ORM against Hibernate Reactive (`orm-vs-reactive`, `run_OrmVsReactive.sh`)
- [`VERTX5_CPU_PROFILE.md`](VERTX5_CPU_PROFILE.md) and [`NETTY_SELECT_ROUNDING.md`](NETTY_SELECT_ROUNDING.md): why Vert.x 5 used more CPU, and the Netty fix (`profile/`, `vertx-repro`)
- [`CONNECTION_STEALING_SUMMARY.md`](CONNECTION_STEALING_SUMMARY.md): a summary of all of them

## How to run a specific JMH benchmark attaching async-profiler?

Install https://github.com/async-profiler/async-profiler/releases/tag/v3.0
and configure it as described as per https://github.com/async-profiler/async-profiler?tab=readme-ov-file#basic-usage.

Build a proper JMH benchmark jar to have more options to run the benchmark.
```shell
$ ./gradlew jmhJar
```

On x86 Linux, run `QueryEntityLazyInitCollectionLoop` attaching wall/cpu and alloc profiling, gc and perfnorm
with 2 forks and 100 loop iterations (it's a specific benchmark parameter):
```shell
$ java -jar basic/target/libs/hibernate-orm-benchmark-basic-1.0-SNAPSHOT-jmh.jar QueryEntityLazyInitCollectionLoop -f 2 -prof gc -prof perfnorm -prof "async:rawCommand=alloc,wall;event=cpu;output=jfr;dir=/tmp;libPath=${ASYNC_PROFILER_HOME}/lib/libasyncProfiler.so" -pcount=100
```
The `rawCommand` parameter has been used to additionally set secondary events which are not yet included into the JMH async-profiler plugin 
i.e. wall-clock profiling and allocation profiling (using the default `alloc` event thresholds, see `JVMTI_EVENT_SAMPLED_OBJECT_ALLOC` at https://docs.oracle.com/en/java/javase/17/docs/specs/jvmti.html i.e. 512KB).

To change the allocation threshold replace `alloc` with `alloc=128` for 128 bytes threshold.

Running the previous command should print something like (assuming Linux perf to be installed):
```
Secondary result "org.hibernate.benchmark.queryl1hit.QueryEntityLazyInitCollectionLoop.test:·async":
Async profiler results:
  /tmp/org.hibernate.benchmark.queryl1hit.QueryEntityLazyInitCollectionLoop.single-Throughput/jfr-cpu.jfr
Async profiler results:
  /tmp/org.hibernate.benchmark.queryl1hit.QueryEntityLazyInitCollectionLoop.single-Throughput/jfr-cpu.jfr
```
and finally the results:
```
Benchmark                                                       (count)   Mode  Cnt          Score       Error      Units
QueryEntityLazyInitCollectionLoop.test                              100  thrpt    6         40.391 ±     1.919      ops/s
QueryEntityLazyInitCollectionLoop.test:CPI                          100  thrpt    2          0.303              clks/insn
QueryEntityLazyInitCollectionLoop.test:IPC                          100  thrpt    2          3.300              insns/clk
QueryEntityLazyInitCollectionLoop.test:L1-dcache-load-misses        100  thrpt    2    5329572.054                   #/op
QueryEntityLazyInitCollectionLoop.test:L1-dcache-loads              100  thrpt    2  173216246.787                   #/op
QueryEntityLazyInitCollectionLoop.test:L1-icache-load-misses        100  thrpt    2     192632.126                   #/op
QueryEntityLazyInitCollectionLoop.test:L1-icache-loads              100  thrpt    2   14780212.318                   #/op
QueryEntityLazyInitCollectionLoop.test:branch-misses                100  thrpt    2     187771.566                   #/op
QueryEntityLazyInitCollectionLoop.test:branches                     100  thrpt    2   92994927.715                   #/op
QueryEntityLazyInitCollectionLoop.test:cycles                       100  thrpt    2  140706458.155                   #/op
QueryEntityLazyInitCollectionLoop.test:dTLB-load-misses             100  thrpt    2      38567.276                   #/op
QueryEntityLazyInitCollectionLoop.test:dTLB-loads                   100  thrpt    2    3969019.541                   #/op
QueryEntityLazyInitCollectionLoop.test:iTLB-load-misses             100  thrpt    2      16704.402                   #/op
QueryEntityLazyInitCollectionLoop.test:iTLB-loads                   100  thrpt    2     414076.369                   #/op
QueryEntityLazyInitCollectionLoop.test:instructions                 100  thrpt    2  464215980.492                   #/op
QueryEntityLazyInitCollectionLoop.test:stalled-cycles-frontend      100  thrpt    2   13056272.972                   #/op
QueryEntityLazyInitCollectionLoop.test:·async                       100  thrpt                 NaN                    ---
QueryEntityLazyInitCollectionLoop.test:·gc.alloc.rate               100  thrpt    6       1155.831 ±    53.937     MB/sec
QueryEntityLazyInitCollectionLoop.test:·gc.alloc.rate.norm          100  thrpt    6   30431129.790 ± 11700.499       B/op
QueryEntityLazyInitCollectionLoop.test:·gc.count                    100  thrpt    6         22.000                 counts
QueryEntityLazyInitCollectionLoop.test:·gc.time                     100  thrpt    6         25.000                     ms
```

The position of the `jfr` files can be used along with the async-profiler converter, to produce the 3 types of flamegraphs we're interested in.

For alloc flamegraphs:
```
$ java -cp ${ASYNC_PROFILER_HOME}/lib/converter.jar jfr2flame --alloc --total /tmp/org.hibernate.benchmark.queryl1hit.QueryEntityLazyInitCollectionLoop.single-Throughput/jfr-cpu.jfr alloc.html
```
The `--total` flag is used to produce flamegraph samples based on the configred allocation threshold, aiming to show the total size of allocations.

Instead, for wall clock and cpu flamegraphs:
```
$ java -cp ${ASYNC_PROFILER_HOME}/lib/converter.jar jfr2flame --state default /tmp/org.hibernate.benchmark.queryl1hit.QueryEntityLazyInitCollectionLoop.single-Throughput/jfr-cpu.jfr cpu.html

$ java -cp ${ASYNC_PROFILER_HOME}/lib/converter.jar jfr2flame --state runnable,sleeping /tmp/org.hibernate.benchmark.queryl1hit.QueryEntityLazyInitCollectionLoop.single-Throughput/jfr-cpu.jfr wall.html
```
Why we're using `--state` here? 

see https://github.com/async-profiler/async-profiler/issues/740#issue-1650739133 for more info: TLDR both wall and cpu events are collected 
using `jdk.ExecutionSample`, hence to distinguish between them, async-profiler uses the `state` field to differentiate between them.

To better understand this, we can type:
```shell
$ jfr print --events jdk.ExecutionSample /tmp/org.hibernate.benchmark.queryl1hit.QueryEntityLazyInitCollectionLoop.test-Throughput-count-100/jfr-cpu.jfr | grep -c STATE_DEFAULT
302
$ jfr summary /tmp/org.hibernate.benchmark.queryl1hit.QueryEntityLazyInitCollectionLoop.test-Throughput-count-100/jfr-cpu.jfr

Version: 2.0
 Chunks: 1
 Start: 2024-05-17 12:32:19 (UTC)
 Duration: 3 s

 Event Type                          Count  Size (bytes) 
=========================================================
 jdk.ObjectAllocationInNewTLAB        7087        141954
 jdk.ExecutionSample                  3509         55622
 jdk.NativeLibrary                      28          2157
 jdk.ActiveSetting                      23           705
 jdk.InitialSystemProperty              17          1066
 jdk.CPULoad                             3            63
 jdk.GCHeapSummary                       3           123
 jdk.Metadata                            1          6952
 jdk.Checkpoint                          1        294850
 jdk.ActiveRecording                     1            75
 jdk.OSInformation                       1           108
 jdk.CPUInformation                      1           365
 jdk.JVMInformation                      1           348
 jdk.ObjectAllocationOutsideTLAB         0             0
 jdk.JavaMonitorEnter                    0             0
 jdk.ThreadPark                          0             0
 profiler.Log                            0             0
 profiler.Window                         0             0
 profiler.LiveObject                     0             0
```
Which shows 2 interesting things:
- as expected, the cpu-related `jdk.ExecutionSample` events are just a part of the overall ones e.g. 302 vs 3509
- there are other events collected by flight-recorder while `async-profiler` agent has been attached
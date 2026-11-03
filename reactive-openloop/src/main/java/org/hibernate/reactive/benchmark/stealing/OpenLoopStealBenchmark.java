/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoop;
import io.vertx.core.DeploymentOptions;

import org.HdrHistogram.Histogram;

/**
 * Measures the CPU cost of connection stealing at a fixed request rate,
 * well below the maximum throughput, where the event loops go to sleep between requests.
 * <p>
 * Each event loop runs a {@link LoadVerticle} that starts requests at a fixed average rate.
 * The benchmark measures, for the event loop threads only:
 * <ul>
 *     <li>the CPU time per request</li>
 *     <li>the voluntary context switches per request: each time an event loop goes to sleep,
 *     something has to wake it up again</li>
 * </ul>
 * and the latency of the requests, so that a pool that steals less can be compared to one that steals more
 * at the same load. It's not a JMH benchmark: JMH threads start the requests from outside the event loops,
 * which adds two thread handoffs to every request and hides the ones caused by stealing.
 * <p>
 * Usage: {@code java -cp <jmh jar> org.hibernate.reactive.benchmark.stealing.OpenLoopStealBenchmark [key=value ...]},
 * see {@link #main(String[])} for the options. Linux only: the context switches come from {@code /proc}.
 */
public final class OpenLoopStealBenchmark {

	private static final Map<String, String> DEFAULTS = new LinkedHashMap<>();

	static {
		DEFAULTS.put( "eventLoops", "4" );
		// shared, perEventLoop (no stealing, poolSize / eventLoops connections per event loop),
		// or a StealPolicyPool policy: always, alwaysActive, preferLocalWait, never
		DEFAULTS.put( "poolType", "shared" );
		DEFAULTS.put( "poolSize", "16" );
		// For the shared pool: spread the connections across this many event loops, like Quarkus (0 = plain Vert.x pool)
		DEFAULTS.put( "poolEventLoopSize", "4" );
		// How long preferLocalWait waits for a connection of the same event loop
		DEFAULTS.put( "localWaitMs", "1" );
		// Total requests per second, for all the event loops
		DEFAULTS.put( "rate", "2000" );
		// How the rate is split between the event loops
		DEFAULTS.put( "weights", "1,1,1,1" );
		DEFAULTS.put( "queriesPerSession", "5" );
		DEFAULTS.put( "warmupSeconds", "10" );
		DEFAULTS.put( "durationSeconds", "20" );
		DEFAULTS.put( "seed", "42" );
		// Count the acquisitions that miss a connection of their event loop because its release is still in the
		// queue of the pool (shared pool only, see AffinityProbe). It adds some work to the pool.
		DEFAULTS.put( "probe", "false" );
	}

	public static void main(String[] args) throws Exception {
		Map<String, String> options = new LinkedHashMap<>( DEFAULTS );
		for ( String arg : args ) {
			String[] keyValue = arg.split( "=", 2 );
			if ( keyValue.length != 2 || !DEFAULTS.containsKey( keyValue[0] ) ) {
				throw new IllegalArgumentException( "Unknown option '" + arg + "', valid options with their defaults: " + DEFAULTS );
			}
			options.put( keyValue[0], keyValue[1] );
		}

		int eventLoops = Integer.parseInt( options.get( "eventLoops" ) );
		String poolType = options.get( "poolType" );
		int poolSize = Integer.parseInt( options.get( "poolSize" ) );
		int poolEventLoopSize = Integer.parseInt( options.get( "poolEventLoopSize" ) );
		long localWaitMs = Long.parseLong( options.get( "localWaitMs" ) );
		double rate = Double.parseDouble( options.get( "rate" ) );
		double[] weights = Arrays.stream( options.get( "weights" ).split( "," ) ).mapToDouble( Double::parseDouble ).toArray();
		int queriesPerSession = Integer.parseInt( options.get( "queriesPerSession" ) );
		long warmupSeconds = Long.parseLong( options.get( "warmupSeconds" ) );
		long durationSeconds = Long.parseLong( options.get( "durationSeconds" ) );
		long seed = Long.parseLong( options.get( "seed" ) );
		boolean probe = Boolean.parseBoolean( options.get( "probe" ) );
		if ( probe && !"shared".equals( poolType ) ) {
			throw new IllegalArgumentException( "probe only works with poolType=shared" );
		}
		if ( weights.length != eventLoops ) {
			throw new IllegalArgumentException( "weights needs one value per event loop: " + options.get( "weights" ) );
		}
		if ( !"shared".equals( poolType ) ) {
			// Only the shared pool uses it, show it in the results
			options.put( "poolEventLoopSize", "0" );
			poolEventLoopSize = 0;
		}
		System.out.println( "Options: " + options );

		var setup = new ReactiveSetup( eventLoops, poolType, poolSize, poolEventLoopSize, localWaitMs );
		try {
			AffinityProbe affinityProbe = probe ? AffinityProbe.install( setup.stealCounter.delegate(), setup.vertx ) : null;
			run( setup, affinityProbe, options, rate, weights, queriesPerSession, warmupSeconds, durationSeconds, seed );
		}
		finally {
			setup.close();
		}
	}

	private static void run(
			ReactiveSetup setup,
			AffinityProbe affinityProbe,
			Map<String, String> options,
			double rate,
			double[] weights,
			int queriesPerSession,
			long warmupSeconds,
			long durationSeconds,
			long seed) throws Exception {
		List<EventLoop> eventLoops = VertxInternals.eventLoops( setup.vertx );
		double weightSum = Arrays.stream( weights ).sum();

		List<LoadVerticle> verticles = new ArrayList<>();
		for ( int i = 0; i < weights.length; i++ ) {
			var verticle = new LoadVerticle( setup.sessionFactory, rate * weights[i] / weightSum, queriesPerSession, seed + i );
			join( setup.vertx.deployVerticle( verticle, new DeploymentOptions() ) );
			verticles.add( verticle );
			int loop = eventLoops.indexOf( VertxInternals.eventLoopOf( verticle.context() ) );
			System.out.printf( "Verticle %d: %.0f requests/s on loop %d%n", i, rate * weights[i] / weightSum, loop );
		}

		System.out.printf( "Warming up for %d s%n", warmupSeconds );
		TimeUnit.SECONDS.sleep( warmupSeconds );

		for ( var verticle : verticles ) {
			join( verticle.startRecording() );
		}
		setup.stealCounter.reset();
		if ( affinityProbe != null ) {
			affinityProbe.reset();
		}
		Snapshot before = Snapshot.take( eventLoops );

		System.out.printf( "Measuring for %d s%n", durationSeconds );
		TimeUnit.SECONDS.sleep( durationSeconds );

		Snapshot after = Snapshot.take( eventLoops );
		long stolen = setup.stealCounter.stolen();
		long acquired = setup.stealCounter.acquired();
		String stealReport = setup.stealCounter.report();
		AffinityProbe.Results affinity = affinityProbe == null ? null : affinityProbe.results();
		Histogram latency = new Histogram( TimeUnit.SECONDS.toNanos( 60 ), 3 );
		long completed = 0;
		long failed = 0;
		int maxInFlight = 0;
		for ( var verticle : verticles ) {
			LoadVerticle.Results results = join( verticle.stopRecording() );
			latency.add( results.histogram() );
			completed += results.completed();
			failed += results.failed();
			maxInFlight = Math.max( maxInFlight, results.maxInFlight() );
			if ( results.firstFailure() != null ) {
				System.out.println( "A request failed:" );
				results.firstFailure().printStackTrace( System.out );
			}
		}
		stopLoad( verticles );

		double seconds = ( after.wallNs - before.wallNs ) / 1e9;
		long loopCpuNs = 0;
		long loopSleeps = 0;
		long loopPreemptions = 0;
		double busiestLoopCpu = 0;
		for ( int i = 0; i < eventLoops.size(); i++ ) {
			long cpuNs = after.loops[i].cpuNs - before.loops[i].cpuNs;
			loopCpuNs += cpuNs;
			loopSleeps += after.loops[i].voluntarySwitches - before.loops[i].voluntarySwitches;
			loopPreemptions += after.loops[i].involuntarySwitches - before.loops[i].involuntarySwitches;
			busiestLoopCpu = Math.max( busiestLoopCpu, cpuNs / 1e9 / seconds );
		}
		long processCpuNs = after.processCpuNs - before.processCpuNs;
		double perRequest = completed == 0 ? Double.NaN : 1.0 / completed;

		System.out.println();
		System.out.println( "=== Steal: " + stealReport );
		System.out.printf( "Requests: %d completed (%.0f/s), %d failed, max %d in flight on one event loop%n",
				completed, completed / seconds, failed, maxInFlight );
		System.out.printf( "Event loops CPU: %.1f%% of a core in total, %.1f%% for the busiest one%n",
				100.0 * loopCpuNs / 1e9 / seconds, 100.0 * busiestLoopCpu );
		System.out.printf( "Per request: %.1f us event loop CPU, %.1f us process CPU, %.2f event loop sleeps, %.3f preemptions%n",
				loopCpuNs / 1e3 * perRequest, processCpuNs / 1e3 * perRequest, loopSleeps * perRequest, loopPreemptions * perRequest );
		System.out.printf( "Latency (us): p50 %.0f, p90 %.0f, p99 %.0f, p99.9 %.0f, max %.0f%n",
				latency.getValueAtPercentile( 50 ) / 1e3, latency.getValueAtPercentile( 90 ) / 1e3,
				latency.getValueAtPercentile( 99 ) / 1e3, latency.getValueAtPercentile( 99.9 ) / 1e3,
				latency.getMaxValue() / 1e3 );

		String affinityResult = "";
		if ( affinity != null ) {
			Histogram wait = affinity.timeToLocalRelease();
			double noLocalFreePct = affinity.acquisitions() == 0 ? 0 : 100.0 * affinity.noLocalFree() / affinity.acquisitions();
			double missedPct = affinity.noLocalFree() == 0 ? 0 : 100.0 * affinity.missed() / affinity.noLocalFree();
			System.out.printf( "Pool: %d acquisitions, %.1f%% found no free connection on their event loop,"
							+ " %.1f%% of those while the release of one was in the queue of the pool%n",
					affinity.acquisitions(), noLocalFreePct, missedPct );
			System.out.printf( "Time until the release of a connection of the same event loop (us): p50 %.0f, p90 %.0f, p99 %.0f%n",
					wait.getValueAtPercentile( 50 ) / 1e3, wait.getValueAtPercentile( 90 ) / 1e3, wait.getValueAtPercentile( 99 ) / 1e3 );
			affinityResult = String.format( " noLocalFreePct=%.1f missedPct=%.1f toLocalReleaseP50Us=%.0f toLocalReleaseP90Us=%.0f toLocalReleaseP99Us=%.0f",
					noLocalFreePct, missedPct, wait.getValueAtPercentile( 50 ) / 1e3, wait.getValueAtPercentile( 90 ) / 1e3,
					wait.getValueAtPercentile( 99 ) / 1e3 );
		}

		// One line with everything, for scripts
		var result = new StringBuilder( "RESULT" );
		options.forEach( (key, value) -> result.append( ' ' ).append( key ).append( '=' ).append( value ) );
		result.append( String.format(
				" achievedRate=%.0f failed=%d stolenPct=%.1f loopCpuPct=%.1f busiestLoopCpuPct=%.1f"
						+ " loopCpuUsPerReq=%.1f processCpuUsPerReq=%.1f loopSleepsPerReq=%.2f preemptionsPerReq=%.3f"
						+ " p50Us=%.0f p90Us=%.0f p99Us=%.0f p999Us=%.0f maxUs=%.0f",
				completed / seconds, failed, acquired == 0 ? 0.0 : 100.0 * stolen / acquired,
				100.0 * loopCpuNs / 1e9 / seconds, 100.0 * busiestLoopCpu,
				loopCpuNs / 1e3 * perRequest, processCpuNs / 1e3 * perRequest, loopSleeps * perRequest, loopPreemptions * perRequest,
				latency.getValueAtPercentile( 50 ) / 1e3, latency.getValueAtPercentile( 90 ) / 1e3,
				latency.getValueAtPercentile( 99 ) / 1e3, latency.getValueAtPercentile( 99.9 ) / 1e3,
				latency.getMaxValue() / 1e3
		) );
		result.append( affinityResult );
		System.out.println( result );
	}

	private static void stopLoad(List<LoadVerticle> verticles) throws InterruptedException {
		for ( var verticle : verticles ) {
			join( verticle.stopLoad() );
		}
		// Let the running requests complete before closing the pool
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos( 30 );
		for ( var verticle : verticles ) {
			while ( join( verticle.inFlight() ) > 0 && System.nanoTime() < deadline ) {
				TimeUnit.MILLISECONDS.sleep( 10 );
			}
		}
	}

	private static <T> T join(io.vertx.core.Future<T> future) {
		return future.toCompletionStage().toCompletableFuture().join();
	}

	/**
	 * CPU time and context switches of each event loop thread, and CPU time of the whole process.
	 */
	private record Snapshot(long wallNs, long processCpuNs, LoopSnapshot[] loops) {

		static Snapshot take(List<EventLoop> eventLoops) throws Exception {
			LoopSnapshot[] loops = new LoopSnapshot[eventLoops.size()];
			for ( int i = 0; i < loops.length; i++ ) {
				// Read on the event loop thread itself: that's the only way to know its Linux thread id
				loops[i] = eventLoops.get( i ).submit( LoopSnapshot::current ).get();
			}
			var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
			return new Snapshot( System.nanoTime(), os.getProcessCpuTime(), loops );
		}
	}

	private record LoopSnapshot(long cpuNs, long voluntarySwitches, long involuntarySwitches) {

		static LoopSnapshot current() throws IOException {
			long voluntary = -1;
			long involuntary = -1;
			for ( String line : Files.readAllLines( Path.of( "/proc/thread-self/status" ) ) ) {
				if ( line.startsWith( "voluntary_ctxt_switches:" ) ) {
					voluntary = Long.parseLong( line.substring( line.indexOf( ':' ) + 1 ).trim() );
				}
				else if ( line.startsWith( "nonvoluntary_ctxt_switches:" ) ) {
					involuntary = Long.parseLong( line.substring( line.indexOf( ':' ) + 1 ).trim() );
				}
			}
			return new LoopSnapshot( ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime(), voluntary, involuntary );
		}
	}
}

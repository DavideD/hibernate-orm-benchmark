/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.benchmark.comparison;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hibernate.reactive.benchmark.stealing.ReactiveLoad;

import org.HdrHistogram.Histogram;

/**
 * Compares Hibernate ORM (blocking JDBC, on platform or virtual threads) with Hibernate Reactive under the same
 * open loop load: requests arrive at random times (Poisson) at a fixed average rate, whatever the speed of the system,
 * and each request is a read-only session that runs {@code queriesPerSession} {@code find}s of random {@code World}s,
 * optionally after a {@code pg_sleep} query that simulates a slower database.
 * The latency is measured from the time each request was scheduled, so a request that waits in a queue counts as slow.
 * <p>
 * {@code mode=fixed} runs at {@code rate} requests per second and reports the CPU of the whole process per request.
 * {@code mode=max} looks for the highest rate that the system sustains: the requests that complete are at least 99%
 * of those scheduled, with a p99 latency of at most {@code sloMs} and no errors. It raises the rate by {@code rampFactor}
 * until a step fails, then bisects between the last step that passed and the first that failed.
 * <p>
 * Usage: {@code java -cp ... OrmVsReactiveBenchmark [key=value ...]}, see {@link #DEFAULTS} for the options.
 * Linux only for the optional PostgreSQL CPU, which reads a cgroup.
 */
public final class OrmVsReactiveBenchmark {

	private static final Map<String, String> DEFAULTS = new LinkedHashMap<>();

	static {
		// orm: blocking Session + pgjdbc + Agroal, reactive: Hibernate Reactive + the Vert.x PostgreSQL client
		DEFAULTS.put( "engine", "reactive" );
		// orm: platform (a fixed pool of `threads` platform threads) or virtual (a virtual thread for each request)
		DEFAULTS.put( "executor", "platform" );
		// orm, platform executor: size of the thread pool (0 = poolSize)
		DEFAULTS.put( "threads", "0" );
		// reactive: shared, localWait or perEventLoop
		DEFAULTS.put( "poolType", "shared" );
		DEFAULTS.put( "localWaitUs", "1000" );
		DEFAULTS.put( "nativeTransport", "false" );
		// reactive: PgConnectOptions#setPipeliningLimit (0 = the default of the client)
		DEFAULTS.put( "pipeliningLimit", "0" );
		// reactive: number of event loops (0 = the processors the JVM can use, which taskset limits)
		DEFAULTS.put( "eventLoops", "0" );
		// Connections of the pool, for both engines
		DEFAULTS.put( "poolSize", "8" );
		DEFAULTS.put( "queriesPerSession", "5" );
		// Cache the prepared statements: the Vert.x client cache, and the pgjdbc one with server-side prepared statements.
		// Both are on by default here; false is only there to see what they save
		DEFAULTS.put( "preparedStatements", "true" );
		// Time the database sleeps at the start of each session (pg_sleep), in ms (0 = no sleep)
		DEFAULTS.put( "dbLatencyMs", "0" );
		// fixed or max
		DEFAULTS.put( "mode", "max" );
		// fixed: requests per second
		DEFAULTS.put( "rate", "2000" );
		DEFAULTS.put( "warmupSeconds", "15" );
		// fixed: length of the measurement
		DEFAULTS.put( "durationSeconds", "20" );
		// max: p99 latency that a step must stay under, in ms
		DEFAULTS.put( "sloMs", "10" );
		// max: rate of the first step, and the factor between steps before the first failure
		DEFAULTS.put( "startRate", "1000" );
		DEFAULTS.put( "rampFactor", "1.5" );
		// max: the search stops when the first failed rate is less than this much over the highest rate that passed
		DEFAULTS.put( "precision", "1.04" );
		// max: unrecorded seconds at the new rate before each step, and the length of the step
		DEFAULTS.put( "settleSeconds", "1" );
		DEFAULTS.put( "stepSeconds", "5" );
		DEFAULTS.put( "seed", "42" );
		// Optional: the cpu.stat file of the cgroup of the PostgreSQL container, to report its CPU
		DEFAULTS.put( "pgCpuStat", "" );
	}

	private static final long DRAIN_TIMEOUT_MS = 30_000;

	// All the options, and the ones to show in the output
	private final Map<String, String> all;
	private final Map<String, String> shown;
	private final LoadEngine engine;
	private final Path pgCpuStat;
	private final double sloNs;

	private OrmVsReactiveBenchmark(Map<String, String> all, Map<String, String> shown, LoadEngine engine) {
		this.all = all;
		this.shown = shown;
		this.engine = engine;
		String pg = all.get( "pgCpuStat" );
		this.pgCpuStat = pg.isEmpty() ? null : Path.of( pg );
		this.sloNs = Double.parseDouble( all.get( "sloMs" ) ) * 1e6;
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
		String engineType = options.get( "engine" );
		int processors = Runtime.getRuntime().availableProcessors();
		int poolSize = Integer.parseInt( options.get( "poolSize" ) );
		int eventLoops = Integer.parseInt( options.get( "eventLoops" ) );
		if ( eventLoops == 0 ) {
			eventLoops = processors;
			options.put( "eventLoops", String.valueOf( eventLoops ) );
		}
		int threads = Integer.parseInt( options.get( "threads" ) );
		if ( threads == 0 ) {
			threads = poolSize;
			options.put( "threads", String.valueOf( threads ) );
		}
		Map<String, String> all = new LinkedHashMap<>( options );
		// Only show the options that the engine uses
		if ( "orm".equals( engineType ) ) {
			for ( String key : new String[] { "poolType", "localWaitUs", "nativeTransport", "pipeliningLimit", "eventLoops" } ) {
				options.remove( key );
			}
			if ( "virtual".equals( options.get( "executor" ) ) ) {
				options.remove( "threads" );
			}
		}
		else {
			options.remove( "executor" );
			options.remove( "threads" );
			if ( !"localWait".equals( options.get( "poolType" ) ) ) {
				options.remove( "localWaitUs" );
			}
		}
		if ( "fixed".equals( options.get( "mode" ) ) ) {
			for ( String key : new String[] { "sloMs", "startRate", "rampFactor", "precision", "settleSeconds", "stepSeconds" } ) {
				options.remove( key );
			}
		}
		else {
			options.remove( "rate" );
			options.remove( "durationSeconds" );
		}
		options.remove( "pgCpuStat" );
		options.put( "processors", String.valueOf( processors ) );
		System.out.println( "Options: " + options );

		int queries = Integer.parseInt( options.get( "queriesPerSession" ) );
		double dbSleepMs = Double.parseDouble( options.get( "dbLatencyMs" ) );
		boolean preparedStatements = Boolean.parseBoolean( options.get( "preparedStatements" ) );
		long seed = Long.parseLong( options.get( "seed" ) );
		LoadEngine engine = switch ( engineType ) {
			case "orm" -> new OrmEngine( poolSize, options.get( "executor" ), threads, queries, dbSleepMs, preparedStatements, seed );
			case "reactive" -> new ReactiveEngine( eventLoops, options.get( "poolType" ), poolSize,
					Long.parseLong( options.getOrDefault( "localWaitUs", "1000" ) ), Boolean.parseBoolean( options.get( "nativeTransport" ) ),
					Integer.parseInt( options.get( "pipeliningLimit" ) ), queries, dbSleepMs, preparedStatements, seed );
			default -> throw new IllegalArgumentException( "Unknown engine: " + engineType + " (valid: orm, reactive)" );
		};
		try (engine) {
			var benchmark = new OrmVsReactiveBenchmark( all, options, engine );
			if ( "fixed".equals( options.get( "mode" ) ) ) {
				benchmark.runFixed();
			}
			else {
				benchmark.runMax();
			}
		}
		// The Vert.x and Agroal threads are daemons or closed, but don't wait for anything else
		System.exit( 0 );
	}

	private void runFixed() throws Exception {
		double rate = Double.parseDouble( all.get( "rate" ) );
		long warmupSeconds = Long.parseLong( all.get( "warmupSeconds" ) );
		System.out.printf( "Warming up for %d s at %.0f requests/s%n", warmupSeconds, rate );
		engine.setRate( rate );
		TimeUnit.SECONDS.sleep( warmupSeconds );
		Step step = step( rate, 0, Long.parseLong( all.get( "durationSeconds" ) ), true );
		System.out.println( step.describe() );
		System.out.println( "RESULT" + options( shown ) + step.fields() );
	}

	private void runMax() throws Exception {
		double rate = Double.parseDouble( all.get( "startRate" ) );
		double factor = Double.parseDouble( all.get( "rampFactor" ) );
		double precision = Double.parseDouble( all.get( "precision" ) );
		long warmupSeconds = Long.parseLong( all.get( "warmupSeconds" ) );
		long settleSeconds = Long.parseLong( all.get( "settleSeconds" ) );
		long stepSeconds = Long.parseLong( all.get( "stepSeconds" ) );

		System.out.printf( "Warming up for %d s at %.0f requests/s%n", warmupSeconds, rate );
		engine.setRate( rate );
		TimeUnit.SECONDS.sleep( warmupSeconds );

		Step best = null;
		double firstFail = Double.POSITIVE_INFINITY;
		String limit = "none";
		int steps = 0;
		while ( steps < 40 ) {
			Step step = step( rate, settleSeconds, stepSeconds, false );
			steps++;
			System.out.println( "STEP" + options( Map.of( "step", String.valueOf( steps ) ) ) + step.fields() );
			System.out.println( step.describe() );
			if ( !step.passes( sloNs ) ) {
				// A step can fail for a reason that has nothing to do with the rate: a GC, a compilation, the box.
				// A rate that fails twice in a row is too high.
				System.out.println( "Failed (" + step.failure( sloNs ) + "), trying the same rate again" );
				drain();
				step = step( rate, settleSeconds, stepSeconds, false );
				steps++;
				System.out.println( "STEP" + options( Map.of( "step", String.valueOf( steps ), "retry", "true" ) ) + step.fields() );
				System.out.println( step.describe() );
			}
			double highestPass = best == null ? 0 : best.targetRate;
			if ( step.passes( sloNs ) ) {
				if ( best == null || step.targetRate > best.targetRate ) {
					best = step;
				}
				highestPass = best.targetRate;
				rate = Double.isInfinite( firstFail ) ? rate * factor : ( highestPass + firstFail ) / 2;
			}
			else {
				firstFail = Math.min( firstFail, step.targetRate );
				limit = step.failure( sloNs );
				rate = best == null ? rate / factor : ( highestPass + firstFail ) / 2;
				if ( best == null && rate < 20 ) {
					break;
				}
			}
			if ( best != null && firstFail < best.targetRate * precision ) {
				break;
			}
			// Let the backlog of a step that failed drain, so that it doesn't count in the next one
			drain();
		}
		if ( best == null ) {
			System.out.println( "RESULT" + options( shown ) + " maxRate=0 steps=" + steps );
			return;
		}
		System.gc();
		System.out.println( "RESULT" + options( shown ) + String.format( Locale.ROOT, " maxRate=%.0f steps=%d limit=%s", best.targetRate, steps, limit )
				+ best.fields().replaceFirst( "^ targetRate=\\S+", "" ) + String.format( Locale.ROOT, " heapMbAfterGc=%.0f", usedHeapMb() ) );
	}

	private void drain() throws InterruptedException {
		engine.setRate( 0 );
		int left = engine.drain( DRAIN_TIMEOUT_MS );
		if ( left > 0 ) {
			System.out.println( "WARNING: " + left + " requests were still running after " + DRAIN_TIMEOUT_MS + " ms" );
		}
		TimeUnit.MILLISECONDS.sleep( 500 );
	}

	/**
	 * Run at {@code rate} (already set, or set now) for {@code settleSeconds}, then record for {@code seconds}.
	 */
	private Step step(double rate, long settleSeconds, long seconds, boolean heap) throws Exception {
		if ( settleSeconds > 0 ) {
			engine.setRate( rate );
			TimeUnit.SECONDS.sleep( settleSeconds );
		}
		var threadBean = ManagementFactory.getThreadMXBean();
		var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		threadBean.resetPeakThreadCount();
		engine.startRecording();
		long startNs = System.nanoTime();
		long cpuStart = os.getProcessCpuTime();
		long gcStart = gcMillis();
		long pgStart = pgCpuMicros();
		CpuUsage cpusStart = CpuUsage.take();
		TimeUnit.SECONDS.sleep( seconds );
		CpuUsage cpusEnd = CpuUsage.take();
		long cpuNs = os.getProcessCpuTime() - cpuStart;
		long wallNs = System.nanoTime() - startNs;
		long gcMs = gcMillis() - gcStart;
		long pgMicros = pgCpuMicros() - pgStart;
		int threads = threadBean.getThreadCount();
		int peakThreads = threadBean.getPeakThreadCount();
		double heapMb = Double.NaN;
		if ( heap ) {
			System.gc();
			heapMb = usedHeapMb();
		}
		ReactiveLoad.Recording recording = engine.stopRecording();
		if ( recording.firstFailure() != null ) {
			System.out.println( "A request failed:" );
			recording.firstFailure().printStackTrace( System.out );
		}
		double secs = wallNs / 1e9;
		return new Step( rate, secs, cpuNs, cpusEnd.busyCores( cpusStart, secs ), cpusEnd.softirqCores( cpusStart, secs ), gcMs,
				pgStart < 0 ? -1 : pgMicros, threads, peakThreads, heapMb, recording );
	}

	private static long gcMillis() {
		long total = 0;
		for ( GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans() ) {
			total += Math.max( 0, gc.getCollectionTime() );
		}
		return total;
	}

	private static double usedHeapMb() {
		var memory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
		return memory.getUsed() / 1024.0 / 1024.0;
	}

	/**
	 * @return the CPU time of the PostgreSQL container, or -1
	 */
	private long pgCpuMicros() {
		if ( pgCpuStat == null ) {
			return -1;
		}
		try {
			for ( String line : Files.readAllLines( pgCpuStat ) ) {
				if ( line.startsWith( "usage_usec " ) ) {
					return Long.parseLong( line.substring( "usage_usec ".length() ).trim() );
				}
			}
		}
		catch (IOException e) {
			throw new IllegalStateException( "Can't read " + pgCpuStat, e );
		}
		return -1;
	}

	private static String options(Map<String, String> options) {
		var result = new StringBuilder();
		options.forEach( (key, value) -> result.append( ' ' ).append( key ).append( '=' ).append( value ) );
		return result.toString();
	}

	/**
	 * What was measured while running at one rate.
	 *
	 * @param targetRate the rate that was set
	 * @param cpuNs CPU time of the whole process (all its threads, the GC and the JIT)
	 * @param busyCores average number of cores that were busy among the CPUs of the JVM, softirqCores the part spent in the
	 * kernel's soft interrupts (see {@link CpuUsage})
	 * @param pgCpuMicros CPU time of PostgreSQL, or -1
	 * @param threads platform threads alive at the end (the virtual threads aren't included), peakThreads the most during the step
	 */
	private record Step(double targetRate, double seconds, long cpuNs, double busyCores, double softirqCores, long gcMs, long pgCpuMicros, int threads, int peakThreads,
						double heapMb, ReactiveLoad.Recording recording) {

		double achievedRate() {
			return recording.completed() / seconds;
		}

		double scheduledRate() {
			return recording.scheduled() / seconds;
		}

		// The requests that completed during the step, over the ones scheduled during it. It's not the rate that was set:
		// the random arrivals don't give exactly that rate in a short step.
		double completedRatio() {
			return recording.scheduled() == 0 ? 0 : (double) recording.completed() / recording.scheduled();
		}

		double p99Ns() {
			return recording.latency().getValueAtPercentile( 99 );
		}

		boolean passes(double sloNs) {
			return recording.failed() == 0 && completedRatio() >= 0.99 && p99Ns() <= sloNs;
		}

		String failure(double sloNs) {
			if ( recording.failed() > 0 ) {
				return "errors";
			}
			if ( completedRatio() < 0.99 ) {
				return "throughput";
			}
			return "p99";
		}

		String describe() {
			Histogram h = recording.latency();
			return String.format( Locale.ROOT,
					"Rate %.0f: %.0f scheduled/s, %.0f completed/s (%.1f%%), %d failed; p50 %.0f us, p99 %.0f us, p99.9 %.0f us;"
							+ " %.1f us CPU per request (%.2f cores, %.2f of %d CPUs busy), %d threads, GC %d ms",
					targetRate, scheduledRate(), achievedRate(), 100 * completedRatio(), recording.failed(),
					h.getValueAtPercentile( 50 ) / 1e3, h.getValueAtPercentile( 99 ) / 1e3, h.getValueAtPercentile( 99.9 ) / 1e3,
					cpuNs / 1e3 / Math.max( 1, recording.completed() ), cpuNs / 1e9 / seconds, busyCores, CpuUsage.cpuCount(), threads, gcMs );
		}

		String fields() {
			Histogram h = recording.latency();
			long completed = Math.max( 1, recording.completed() );
			return String.format( Locale.ROOT,
					" targetRate=%.0f scheduledRate=%.0f achievedRate=%.0f completedPct=%.1f failed=%d"
							+ " p50Us=%.0f p90Us=%.0f p99Us=%.0f p999Us=%.0f maxUs=%.0f"
							+ " cpuUsPerReq=%.1f cpuCores=%.2f busyUsPerReq=%.1f busyCores=%.2f softirqCores=%.2f pgCpuCores=%.2f gcMsPerSec=%.1f threads=%d peakThreads=%d maxInFlight=%d%s",
					targetRate, scheduledRate(), achievedRate(), 100 * completedRatio(), recording.failed(),
					h.getValueAtPercentile( 50 ) / 1e3, h.getValueAtPercentile( 90 ) / 1e3, h.getValueAtPercentile( 99 ) / 1e3,
					h.getValueAtPercentile( 99.9 ) / 1e3, h.getMaxValue() / 1e3,
					cpuNs / 1e3 / completed, cpuNs / 1e9 / seconds, busyCores * 1e6 / Math.max( 1, achievedRate() ), busyCores, softirqCores, pgCpuMicros < 0 ? Double.NaN : pgCpuMicros / 1e6 / seconds,
					gcMs / seconds, threads, peakThreads, recording.maxInFlight(),
					Double.isNaN( heapMb ) ? "" : String.format( Locale.ROOT, " heapMbAfterGc=%.0f", heapMb ) );
		}
	}
}

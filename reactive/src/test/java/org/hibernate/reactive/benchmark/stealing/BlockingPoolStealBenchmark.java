package org.hibernate.reactive.benchmark.stealing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;

import org.HdrHistogram.Histogram;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Fork(2)
@Threads(2)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
public class BlockingPoolStealBenchmark {

	@Param({"0", "5", "10"})
	int sleepMs;

	private SessionFactory sf;

	@Setup(Level.Trial)
	public void setup() {
		var config = new Configuration();
		config.addAnnotatedClass( World.class );
		config.setProperty( AvailableSettings.JAKARTA_JDBC_URL,
				"jdbc:postgresql://localhost/hibernate_orm_test?preparedStatementCacheQueries=0" );
		config.setProperty( AvailableSettings.JAKARTA_JDBC_USER, "hibernate_orm_test" );
		config.setProperty( AvailableSettings.JAKARTA_JDBC_PASSWORD, "hibernate_orm_test" );
		config.setProperty( AvailableSettings.SHOW_SQL, "false" );
		config.setProperty( AvailableSettings.FORMAT_SQL, "false" );
		config.setProperty( AvailableSettings.HBM2DDL_AUTO, "create" );
		config.setProperty( AvailableSettings.STATEMENT_BATCH_SIZE, "0" );
		config.setProperty( "hibernate.generate_statistics", "false" );
		config.setProperty( "hibernate.agroal.maxSize", "2" );
		config.setProperty( "hibernate.agroal.minSize", "2" );
		config.setProperty( "hibernate.agroal.acquisitionTimeout_s", "30" );

		sf = config.buildSessionFactory();

		populateWorlds();
	}

	private void populateWorlds() {
		var em = sf.createEntityManager();
		em.getTransaction().begin();
		for ( int i = 0; i < 10_000; i++ ) {
			var world = new World();
			world.setId( i + 1 );
			world.setRandomNumber( ThreadLocalRandom.current().nextInt( 10_000 ) );
			em.persist( world );
			if ( i % 1000 == 0 ) {
				em.flush();
				em.clear();
			}
		}
		em.getTransaction().commit();
		em.close();
	}

	@TearDown(Level.Trial)
	public void teardown() {
		if ( sf != null ) {
			sf.getSchemaManager().dropMappedObjects( false );
			sf.close();
		}
	}

	@State(Scope.Thread)
	public static class LatencyState {
		private static final long SLOW_THRESHOLD_NS = TimeUnit.MILLISECONDS.toNanos( 1 );
		// With sleepMs >= 1 every operation is slow: only keep the first ones
		private static final int MAX_SLOW_OPS = 100;

		@Param({"0"})
		long targetInterArrivalNs;

		Histogram histogram;
		long nextExpectedStartNs;
		long iterationStartNs;
		List<long[]> slowOps;
		long slowOpsCount;

		@Setup(Level.Iteration)
		public void setup() {
			histogram = new Histogram( TimeUnit.SECONDS.toNanos( 30 ), 3 );
			nextExpectedStartNs = 0;
			iterationStartNs = System.nanoTime();
			slowOps = new ArrayList<>( MAX_SLOW_OPS );
			slowOpsCount = 0;
		}

		public long awaitExpectedStart() {
			if ( targetInterArrivalNs <= 0 ) {
				return System.nanoTime();
			}
			long now = System.nanoTime();
			if ( nextExpectedStartNs == 0 ) {
				nextExpectedStartNs = now;
			}
			long expected = nextExpectedStartNs;
			nextExpectedStartNs += targetInterArrivalNs;
			while ( System.nanoTime() < expected ) {
				Thread.onSpinWait();
			}
			return expected;
		}

		public void recordLatency(long expectedStartNs) {
			long endNs = System.nanoTime();
			long elapsed = endNs - expectedStartNs;
			histogram.recordValue( Math.max( elapsed, 0 ) );
			if ( elapsed > SLOW_THRESHOLD_NS ) {
				if ( slowOpsCount++ < MAX_SLOW_OPS ) {
					long offsetNs = endNs - iterationStartNs;
					slowOps.add( new long[]{ offsetNs, elapsed } );
				}
			}
		}

		@TearDown(Level.Iteration)
		public void report() {
			if ( histogram.getTotalCount() > 0 ) {
				System.out.println();
				System.out.println( "=== Blocking ORM - HDR Histogram (microseconds) ===" );
				System.out.printf( "  Count:  %d%n", histogram.getTotalCount() );
				System.out.printf( "  Mean:   %.1f%n", histogram.getMean() / 1000.0 );
				System.out.printf( "  P50:    %.1f%n", histogram.getValueAtPercentile( 50.0 ) / 1000.0 );
				System.out.printf( "  P90:    %.1f%n", histogram.getValueAtPercentile( 90.0 ) / 1000.0 );
				System.out.printf( "  P99:    %.1f%n", histogram.getValueAtPercentile( 99.0 ) / 1000.0 );
				System.out.printf( "  P99.9:  %.1f%n", histogram.getValueAtPercentile( 99.9 ) / 1000.0 );
				System.out.printf( "  Max:    %.1f%n", histogram.getMaxValue() / 1000.0 );
				if ( !slowOps.isEmpty() ) {
					System.out.printf( "  Slow (>1ms): %d ops%n", slowOpsCount );
					System.out.printf( "  First %d slow ops (offset_ms, latency_ms):%n", slowOps.size() );
					for ( var op : slowOps ) {
						System.out.printf( "    t=%.1f  lat=%.1f%n", op[0] / 1_000_000.0, op[1] / 1_000_000.0 );
					}
				}
				System.out.println();
				histogram.outputPercentileDistribution( System.out, 1000.0 );
			}
		}
	}

	// -- Benchmark methods --

	private void sleepThenFind(int sleepMs) {
		var em = sf.createEntityManager();
		try {
			em.createNativeQuery( "SELECT 1 FROM pg_sleep(" + (sleepMs / 1000.0) + ")" )
					.getSingleResult();
//			int id = ThreadLocalRandom.current().nextInt( 10_000 ) + 1;
//			em.find( World.class, id );
		}
		finally {
			em.close();
		}
	}

	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public void throughput() {
		sleepThenFind( sleepMs );
	}

	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public void latency(LatencyState lat) {
		long expectedStart = lat.awaitExpectedStart();

		sleepThenFind( sleepMs );

		lat.recordLatency( expectedStart );
	}

	public static void main(String[] args) {
		var bench = new BlockingPoolStealBenchmark();
		bench.sleepMs = 0;
		bench.setup();
		try {
			for ( int i = 0; i < 10; i++ ) {
				bench.throughput();
			}
			System.out.println( "Completed 10 invocations" );
		}
		finally {
			bench.teardown();
		}
	}
}

package org.hibernate.reactive.benchmark.stealing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.vertx.VertxInstance;

import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;

import org.HdrHistogram.Histogram;
import org.openjdk.jmh.annotations.AuxCounters;
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

@State(Scope.Benchmark)
@Fork(2)
@Threads(2)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
public class ReactivePoolStealBenchmark {

	@Param({"2", "4", "8"})
	int eventLoopCount;

	@Param({"0", "5", "10"})
	int sleepMs;

	// -1 = round-robin (each thread gets its own verticle), 0+ = all threads target that verticle
	@Param({"-1"})
	int targetVerticle;

	private Mutiny.SessionFactory sessionFactory;
	private Vertx vertx;
	private final AtomicInteger verticleIndex = new AtomicInteger();
	private List<String> deploymentIds;

	@Setup(Level.Trial)
	public void setup() {
		var vertxOptions = new VertxOptions().setEventLoopPoolSize( eventLoopCount );
		vertx = Vertx.vertx( vertxOptions );

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
		config.setProperty( AvailableSettings.POOL_SIZE, "2" );

		var srb = new ReactiveServiceRegistryBuilder()
				.addService( VertxInstance.class, (VertxInstance) () -> vertx )
				.applySettings( config.getProperties() );

		sessionFactory = config.buildSessionFactory( srb.build() )
				.unwrap( Mutiny.SessionFactory.class );

		populateWorlds();
		deployVerticles();
	}

	private void populateWorlds() {
		final int batchSize = 500;
		for ( int batch = 0; batch < 10_000 / batchSize; batch++ ) {
			final int start = batch * batchSize;
			sessionFactory.withTransaction( (session, tx) -> {
				Uni<Void> chain = Uni.createFrom().voidItem();
				for ( int i = start; i < start + batchSize; i++ ) {
					final World world = new World();
					world.setId( i + 1 );
					world.setRandomNumber( ThreadLocalRandom.current().nextInt( 10_000 ) );
					chain = chain.chain( () -> session.persist( world ) );
				}
				return chain.chain( session::flush );
			} ).await().atMost( Duration.ofMinutes( 2 ) );
		}
	}

	private void deployVerticles() {
		deploymentIds = new ArrayList<>( eventLoopCount );
		for ( int i = 0; i < eventLoopCount; i++ ) {
			var verticle = new WorkerVerticle( sessionFactory, i );
			var id = vertx.deployVerticle( verticle, new DeploymentOptions() )
					.toCompletionStage().toCompletableFuture().join();
			deploymentIds.add( id );
		}
	}

	@TearDown(Level.Trial)
	public void teardown() {
		if ( deploymentIds != null ) {
			for ( var id : deploymentIds ) {
				vertx.undeploy( id ).toCompletionStage().toCompletableFuture().join();
			}
		}
		if ( sessionFactory != null ) {
			sessionFactory.close();
		}
		if ( vertx != null ) {
			vertx.close().toCompletionStage().toCompletableFuture().join();
		}
	}

	@State(Scope.Thread)
	@AuxCounters(AuxCounters.Type.OPERATIONS)
	public static class Counters {
		public long queries;
	}

	@State(Scope.Thread)
	public static class ThreadContext {
		String verticleAddress;

		@Setup(Level.Trial)
		public void setup(ReactivePoolStealBenchmark bench) {
			int idx;
			if ( bench.targetVerticle >= 0 ) {
				idx = bench.targetVerticle;
			}
			else {
				idx = bench.verticleIndex.getAndIncrement() % bench.eventLoopCount;
			}
			verticleAddress = WorkerVerticle.ADDRESS_PREFIX + idx;
		}
	}

	@State(Scope.Thread)
	public static class LatencyState {
		private static final long SLOW_THRESHOLD_NS = TimeUnit.MILLISECONDS.toNanos( 1 );

		@Param({"0"})
		long targetInterArrivalNs;

		Histogram histogram;
		long nextExpectedStartNs;
		long iterationStartNs;
		CopyOnWriteArrayList<long[]> slowOps;

		@Setup(Level.Iteration)
		public void setup() {
			histogram = new Histogram( TimeUnit.SECONDS.toNanos( 30 ), 3 );
			nextExpectedStartNs = 0;
			iterationStartNs = System.nanoTime();
			slowOps = new CopyOnWriteArrayList<>();
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
				long offsetNs = endNs - iterationStartNs;
				slowOps.add( new long[]{ offsetNs, elapsed } );
			}
		}

		@TearDown(Level.Iteration)
		public void report() {
			if ( histogram.getTotalCount() > 0 ) {
				System.out.println();
				System.out.println( "=== Reactive - HDR Histogram (microseconds) ===" );
				System.out.printf( "  Count:  %d%n", histogram.getTotalCount() );
				System.out.printf( "  Mean:   %.1f%n", histogram.getMean() / 1000.0 );
				System.out.printf( "  P50:    %.1f%n", histogram.getValueAtPercentile( 50.0 ) / 1000.0 );
				System.out.printf( "  P90:    %.1f%n", histogram.getValueAtPercentile( 90.0 ) / 1000.0 );
				System.out.printf( "  P99:    %.1f%n", histogram.getValueAtPercentile( 99.0 ) / 1000.0 );
				System.out.printf( "  P99.9:  %.1f%n", histogram.getValueAtPercentile( 99.9 ) / 1000.0 );
				System.out.printf( "  Max:    %.1f%n", histogram.getMaxValue() / 1000.0 );
				if ( !slowOps.isEmpty() ) {
					System.out.printf( "  Slow (>1ms): %d ops%n", slowOps.size() );
					System.out.println( "  Slow ops (offset_ms, latency_ms):" );
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

	private void sendToVerticle(String address, int sleepMs) {
		var msg = new JsonObject().put( "sleepMs", sleepMs );
		vertx.eventBus().<String>request( address, msg )
				.toCompletionStage().toCompletableFuture().join();
	}

	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public void throughput(Counters counters, ThreadContext tc) {
		sendToVerticle( tc.verticleAddress, sleepMs );
		counters.queries++;
	}

	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public void latency(Counters counters, LatencyState lat, ThreadContext tc) {
		long expectedStart = lat.awaitExpectedStart();

		sendToVerticle( tc.verticleAddress, sleepMs );
		counters.queries++;

		lat.recordLatency( expectedStart );
	}

	public static void main(String[] args) {
		var bench = new ReactivePoolStealBenchmark();
		bench.eventLoopCount = 2;
		bench.sleepMs = 0;
		bench.setup();

		try {
			for ( int i = 0; i < 10; i++ ) {
				bench.sendToVerticle( WorkerVerticle.ADDRESS_PREFIX + "0", bench.sleepMs );
			}
			System.out.println( "Completed 10 invocations" );
		}
		finally {
			bench.teardown();
		}
	}
}

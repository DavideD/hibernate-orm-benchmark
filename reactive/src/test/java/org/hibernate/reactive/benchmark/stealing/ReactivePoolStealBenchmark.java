package org.hibernate.reactive.benchmark.stealing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.impl.ExternalSqlClientPool;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.vertx.VertxInstance;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;

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

	@Param({"2"})
	int poolSize;

	// When > 0, mimics Quarkus: PoolOptions#setEventLoopSize spreads connections across event loops
	@Param({"0"})
	int poolEventLoopSize;

	// shared = one pool for all the event loops (like Quarkus),
	// perEventLoop = one pool per event loop with poolSize / eventLoopCount connections each (ignores poolEventLoopSize)
	@Param({"shared"})
	String poolType;

	private StealCounter stealCounter;

	private Mutiny.SessionFactory sessionFactory;
	private Vertx vertx;
	private Pool sharedPool;
	private PgConnectOptions connectOptions;
	private PoolOptions poolOptions;
	private final AtomicInteger verticleIndex = new AtomicInteger();
	private List<String> deploymentIds;

	@Setup(Level.Trial)
	public void setup() {
		var vertxOptions = new VertxOptions().setEventLoopPoolSize( eventLoopCount );
		vertx = Vertx.vertx( vertxOptions );

		connectOptions = new PgConnectOptions()
				.setHost( "localhost" )
				.setPort( 5432 )
				.setDatabase( "hibernate_orm_test" )
				.setUser( "hibernate_orm_test" )
				.setPassword( "hibernate_orm_test" )
				.setCachePreparedStatements( false );
		poolOptions = new PoolOptions().setMaxSize( poolSize ).setEventLoopSize( poolEventLoopSize );
		stealCounter = new StealCounter( vertx );
		sharedPool = stealCounter.wrap( createPool() );

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

		var externalPool = new ExternalSqlClientPool(
				sharedPool,
				new SqlStatementLogger( false, false ),
				new SqlExceptionHelper( true )
		);

		var srb = new ReactiveServiceRegistryBuilder()
				.addService( VertxInstance.class, (VertxInstance) () -> vertx )
				.addService( ReactiveConnectionPool.class, externalPool )
				.applySettings( config.getProperties() );

		sessionFactory = config.buildSessionFactory( srb.build() )
				.unwrap( Mutiny.SessionFactory.class );

		populateWorlds();
		discardSetupConnections();
		deployVerticles();
	}

	/**
	 * The connections opened so far (dialect detection, schema creation, {@link #populateWorlds()})
	 * belong to the event loops of the contexts Vert.x created for the JMH thread, not to the verticles.
	 * The pool keeps them open, so they would add steals that the benchmark doesn't intend to measure.
	 */
	private void discardSetupConnections() {
		stealCounter.replace( createPool() )
				.close().toCompletionStage().toCompletableFuture().join();
	}

	private Pool createPool() {
		return switch ( poolType ) {
			case "shared" -> Pool.pool( vertx, connectOptions, poolOptions );
			case "perEventLoop" -> PerEventLoopPool.create( vertx, connectOptions, poolOptions );
			default -> throw new IllegalArgumentException( "Unknown pool type: " + poolType );
		};
	}

	private void populateWorlds() {
		final int batchSize = 500;
		for ( int batch = 0; batch < 10_000 / batchSize; batch++ ) {
			final int start = batch * batchSize;
			// A Uni chain 500 levels deep overflows the event loop stack: use persistAll
			final Object[] worlds = new Object[batchSize];
			for ( int i = 0; i < batchSize; i++ ) {
				final World world = new World();
				world.setId( start + i + 1 );
				world.setRandomNumber( ThreadLocalRandom.current().nextInt( 10_000 ) );
				worlds[i] = world;
			}
			sessionFactory.withTransaction( session -> session.persistAll( worlds ) )
					.await().atMost( Duration.ofMinutes( 2 ) );
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

	@Setup(Level.Iteration)
	public void resetStealCounter() {
		stealCounter.reset();
	}

	@TearDown(Level.Iteration)
	public void reportStealCounter() {
		System.out.println( "\n=== Steal: " + stealCounter.report() );
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
		if ( sharedPool != null ) {
			sharedPool.close().toCompletionStage().toCompletableFuture().join();
		}
		if ( vertx != null ) {
			vertx.close().toCompletionStage().toCompletableFuture().join();
		}
	}

	@State(Scope.Thread)
	public static class ThreadContext {
		String verticleAddress;
		boolean hot;

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
			hot = (idx == 0);
		}
	}

	@State(Scope.Thread)
	public static class LatencyState {
		private static final long SLOW_THRESHOLD_NS = TimeUnit.MILLISECONDS.toNanos( 1 );
		// With sleepMs >= 1 every operation is slow: only keep the first ones
		private static final int MAX_SLOW_OPS = 100;

		// Rate for the "cool" thread (verticle 1+)
		@Param({"0"})
		long targetInterArrivalNs;

		// Rate for the "hot" thread (verticle 0). If 0, uses targetInterArrivalNs.
		@Param({"0"})
		long hotInterArrivalNs;

		Histogram histogram;
		long nextExpectedStartNs;
		long iterationStartNs;
		long effectiveInterArrivalNs;
		List<long[]> slowOps;
		long slowOpsCount;

		@Setup(Level.Iteration)
		public void setup(ThreadContext tc) {
			histogram = new Histogram( TimeUnit.SECONDS.toNanos( 30 ), 3 );
			nextExpectedStartNs = 0;
			iterationStartNs = System.nanoTime();
			effectiveInterArrivalNs = (tc.hot && hotInterArrivalNs > 0)
					? hotInterArrivalNs : targetInterArrivalNs;
			slowOps = new ArrayList<>( MAX_SLOW_OPS );
			slowOpsCount = 0;
		}

		public long awaitExpectedStart() {
			if ( effectiveInterArrivalNs <= 0 ) {
				return System.nanoTime();
			}
			long now = System.nanoTime();
			if ( nextExpectedStartNs == 0 ) {
				nextExpectedStartNs = now;
			}
			long expected = nextExpectedStartNs;
			nextExpectedStartNs += effectiveInterArrivalNs;
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
				System.out.println( "=== Reactive - HDR Histogram (microseconds) ===" );
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

	private void sendToVerticle(String address, int sleepMs) {
		var msg = new JsonObject().put( "sleepMs", sleepMs );
		vertx.eventBus().<String>request( address, msg )
				.toCompletionStage().toCompletableFuture().join();
	}

	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public void throughput(ThreadContext tc) {
		sendToVerticle( tc.verticleAddress, sleepMs );
	}

	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public void latency(LatencyState lat, ThreadContext tc) {
		long expectedStart = lat.awaitExpectedStart();

		sendToVerticle( tc.verticleAddress, sleepMs );

		lat.recordLatency( expectedStart );
	}

	public static void main(String[] args) {
		var bench = new ReactivePoolStealBenchmark();
		bench.eventLoopCount = 2;
		bench.sleepMs = 0;
		bench.poolSize = 2;
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

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.repro;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

/**
 * Vert.x only reproducer: the load of {@code OpenLoopStealBenchmark} without Hibernate.
 * <p>
 * Every event loop runs a verticle that starts requests at a fixed average rate (Poisson arrivals).
 * A request takes a connection from the pool, runs {@code queries} prepared queries one after the other
 * ({@code select id, random_number from repro_world where id = $1}), and gives the connection back.
 * It prints the CPU time of the event loop threads per completed request.
 * <p>
 * Same code for Vert.x 4.5 and 5: {@code -Pstack=vertx5} selects the version.
 * Usage: {@code VertxCpuRepro [key=value ...]}, the defaults are in {@link #DEFAULTS}. Needs PostgreSQL on localhost.
 */
public final class VertxCpuRepro {

	private static final Map<String, String> DEFAULTS = new LinkedHashMap<>();

	static {
		DEFAULTS.put( "eventLoops", "4" );
		// shared: one pool for all the event loops, its connections spread across poolEventLoopSize event loops (like Quarkus)
		// perEventLoop: each verticle creates its own pool, with poolSize / eventLoops connections: no connection is stolen
		DEFAULTS.put( "poolType", "shared" );
		DEFAULTS.put( "poolSize", "4" );
		DEFAULTS.put( "poolEventLoopSize", "4" );
		// Total requests per second for all the event loops
		DEFAULTS.put( "rate", "4000" );
		DEFAULTS.put( "queries", "5" );
		// session: getConnection(), N queries on it, close(). pool: N queries with pool.preparedQuery()
		DEFAULTS.put( "mode", "session" );
		DEFAULTS.put( "cachePreparedStatements", "false" );
		DEFAULTS.put( "warmupSeconds", "10" );
		DEFAULTS.put( "durationSeconds", "20" );
		DEFAULTS.put( "seed", "42" );
		// timer: each verticle starts its requests from a Vert.x timer (a scheduled task of its event loop)
		// thread: a thread outside Vert.x starts them with runOnContext() and no timer exists
		DEFAULTS.put( "generator", "timer" );
		// Prefer the native epoll transport if its jars are on the class path (Vert.x uses NIO by default)
		DEFAULTS.put( "nativeTransport", "false" );
	}

	private static final String QUERY = "select id, random_number from repro_world where id = $1";
	private static final int ROWS = 10_000;

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
		boolean perEventLoop = "perEventLoop".equals( options.get( "poolType" ) );
		int poolSize = Integer.parseInt( options.get( "poolSize" ) );
		int poolEventLoopSize = Integer.parseInt( options.get( "poolEventLoopSize" ) );
		double rate = Double.parseDouble( options.get( "rate" ) );
		int queries = Integer.parseInt( options.get( "queries" ) );
		boolean sessionMode = "session".equals( options.get( "mode" ) );
		long warmupSeconds = Long.parseLong( options.get( "warmupSeconds" ) );
		long durationSeconds = Long.parseLong( options.get( "durationSeconds" ) );
		long seed = Long.parseLong( options.get( "seed" ) );
		boolean timers = "timer".equals( options.get( "generator" ) );
		System.out.println( "Options: " + options );
		System.out.println( "Vert.x: " + Vertx.class.getPackage().getImplementationVersion() );

		var connectOptions = new PgConnectOptions()
				.setHost( "localhost" )
				.setPort( 5432 )
				.setDatabase( "hibernate_orm_test" )
				.setUser( "hibernate_orm_test" )
				.setPassword( "hibernate_orm_test" )
				.setCachePreparedStatements( Boolean.parseBoolean( options.get( "cachePreparedStatements" ) ) );

		Vertx vertx = Vertx.vertx( new VertxOptions().setEventLoopPoolSize( eventLoops )
				.setPreferNativeTransport( Boolean.parseBoolean( options.get( "nativeTransport" ) ) ) );
		System.out.println( "Native transport: " + vertx.isNativeTransportEnabled() );
		try {
			createTable( vertx, connectOptions );

			Pool sharedPool = perEventLoop ? null : Pool.pool( vertx, connectOptions,
					new PoolOptions().setMaxSize( poolSize ).setEventLoopSize( poolEventLoopSize ) );
			var load = new Load();
			List<Verticle> verticles = new ArrayList<>();
			for ( int i = 0; i < eventLoops; i++ ) {
				var verticle = new Verticle( load, sharedPool, connectOptions, poolSize / eventLoops, rate / eventLoops,
						queries, sessionMode, timers, new SplittableRandom( seed + i ) );
				vertx.deployVerticle( verticle ).toCompletionStage().toCompletableFuture().join();
				verticles.add( verticle );
			}

			System.out.printf( "Warming up for %d s%n", warmupSeconds );
			TimeUnit.SECONDS.sleep( warmupSeconds );
			load.recording = true;
			long loopCpuBefore = eventLoopCpuNanos();
			long completedBefore = load.completed.sum();
			long start = System.nanoTime();
			System.out.printf( "Measuring for %d s%n", durationSeconds );
			TimeUnit.SECONDS.sleep( durationSeconds );
			long loopCpuNs = eventLoopCpuNanos() - loopCpuBefore;
			long completed = load.completed.sum() - completedBefore;
			double seconds = ( System.nanoTime() - start ) / 1e9;
			load.recording = false;
			verticles.forEach( Verticle::stopLoad );
			TimeUnit.MILLISECONDS.sleep( 500 );

			System.out.printf( "Requests: %d completed (%.0f/s), %d failed%n", completed, completed / seconds, load.failed.sum() );
			System.out.printf( "Event loops CPU: %.1f%% of a core in total%n", 100.0 * loopCpuNs / 1e9 / seconds );
			var result = new StringBuilder( "RESULT" );
			options.forEach( (key, value) -> result.append( ' ' ).append( key ).append( '=' ).append( value ) );
			result.append( String.format( " achievedRate=%.0f failed=%d loopCpuPct=%.1f loopCpuUsPerReq=%.1f",
					completed / seconds, load.failed.sum(), 100.0 * loopCpuNs / 1e9 / seconds, loopCpuNs / 1e3 / completed ) );
			System.out.println( result );
			if ( sharedPool != null ) {
				sharedPool.close().toCompletionStage().toCompletableFuture().join();
			}
		}
		finally {
			vertx.close().toCompletionStage().toCompletableFuture().join();
		}
	}

	private static void createTable(Vertx vertx, PgConnectOptions connectOptions) {
		Pool pool = Pool.pool( vertx, connectOptions, new PoolOptions().setMaxSize( 1 ) );
		pool.query( "create table if not exists repro_world (id int primary key, random_number int)" ).execute()
				.compose( v -> pool.query( "insert into repro_world select g, (random() * 10000)::int"
						+ " from generate_series(1, " + ROWS + ") g on conflict do nothing" ).execute() )
				.toCompletionStage().toCompletableFuture().join();
		pool.close().toCompletionStage().toCompletableFuture().join();
	}

	/** CPU time of the event loop threads, all of them. */
	private static long eventLoopCpuNanos() {
		var threads = ManagementFactory.getThreadMXBean();
		long total = 0;
		for ( var info : threads.getThreadInfo( threads.getAllThreadIds() ) ) {
			if ( info != null && info.getThreadName().startsWith( "vert.x-eventloop-thread-" ) ) {
				total += threads.getThreadCpuTime( info.getThreadId() );
			}
		}
		return total;
	}

	private static final class Load {
		volatile boolean recording;
		final LongAdder completed = new LongAdder();
		final LongAdder failed = new LongAdder();
	}

	private static final class Verticle extends AbstractVerticle {

		private final Load load;
		private final Pool sharedPool;
		private final PgConnectOptions connectOptions;
		private final int ownPoolSize;
		private final double ratePerSecond;
		private final int queries;
		private final boolean sessionMode;
		private final boolean timers;
		private final SplittableRandom random;
		private Thread generator;

		private Pool pool;
		private long nextArrivalNs;
		private volatile boolean stopped;

		Verticle(Load load, Pool sharedPool, PgConnectOptions connectOptions, int ownPoolSize, double ratePerSecond,
				int queries, boolean sessionMode, boolean timers, SplittableRandom random) {
			this.load = load;
			this.sharedPool = sharedPool;
			this.connectOptions = connectOptions;
			this.ownPoolSize = ownPoolSize;
			this.ratePerSecond = ratePerSecond;
			this.queries = queries;
			this.sessionMode = sessionMode;
			this.timers = timers;
			this.random = random;
		}

		@Override
		public void start() {
			// The connections of a pool created here belong to the event loop of this verticle
			pool = sharedPool != null ? sharedPool : Pool.pool( vertx, connectOptions, new PoolOptions().setMaxSize( ownPoolSize ) );
			nextArrivalNs = System.nanoTime() + nextIntervalNs();
			if ( timers ) {
				scheduleNext();
			}
			else {
				var context = vertx.getOrCreateContext();
				generator = new Thread( () -> generate( context ), "generator" );
				generator.setDaemon( true );
				generator.start();
			}
		}

		// Poisson arrivals from a plain thread: sleeps until the next arrival, then hands the request to the event loop
		private void generate(io.vertx.core.Context context) {
			while ( !stopped ) {
				long wait = nextArrivalNs - System.nanoTime();
				if ( wait > 0 ) {
					java.util.concurrent.locks.LockSupport.parkNanos( wait );
					continue;
				}
				nextArrivalNs += nextIntervalNs();
				context.runOnContext( v -> request() );
			}
		}

		// A timer for each tick, not a periodic one, like LoadVerticle: the requests that are due start together
		private void scheduleNext() {
			long delayMs = Math.max( 1, TimeUnit.NANOSECONDS.toMillis( nextArrivalNs - System.nanoTime() ) );
			vertx.setTimer( delayMs, id -> tick() );
		}

		private void tick() {
			if ( stopped ) {
				return;
			}
			long now = System.nanoTime();
			while ( nextArrivalNs <= now ) {
				request();
				nextArrivalNs += nextIntervalNs();
			}
			scheduleNext();
		}

		private long nextIntervalNs() {
			// Exponential distribution: the intervals of a Poisson process
			return (long) ( -Math.log( 1.0 - random.nextDouble() ) * 1_000_000_000.0 / ratePerSecond );
		}

		void stopLoad() {
			stopped = true;
		}

		private void request() {
			if ( sessionMode ) {
				pool.getConnection().onComplete( ar -> {
					if ( ar.failed() ) {
						done( ar.cause() );
					}
					else {
						query( ar.result(), 0 );
					}
				} );
			}
			else {
				query( null, 0 );
			}
		}

		private void query(SqlConnection connection, int done) {
			if ( done == queries ) {
				if ( connection == null ) {
					done( null );
				}
				else {
					connection.close().onComplete( ar -> done( ar.cause() ) );
				}
				return;
			}
			var tuple = Tuple.of( random.nextInt( ROWS ) + 1 );
			Future<?> result = connection == null
					? pool.preparedQuery( QUERY ).execute( tuple )
					: connection.preparedQuery( QUERY ).execute( tuple );
			result.onComplete( ar -> {
				if ( ar.failed() ) {
					if ( connection != null ) {
						connection.close();
					}
					done( ar.cause() );
				}
				else {
					query( connection, done + 1 );
				}
			} );
		}

		private void done(Throwable failure) {
			if ( load.recording ) {
				( failure == null ? load.completed : load.failed ).increment();
			}
		}
	}
}

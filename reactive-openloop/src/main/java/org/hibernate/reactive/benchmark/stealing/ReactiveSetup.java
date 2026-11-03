/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.impl.ExternalSqlClientPool;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.vertx.VertxInstance;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;

/**
 * Starts Vert.x, the pool and the Hibernate Reactive {@link Mutiny.SessionFactory} used by the pool steal benchmarks,
 * and fills the {@link World} table.
 */
public final class ReactiveSetup {

	static final int WORLD_COUNT = 10_000;

	/**
	 * The settings of the Vert.x PostgreSQL client that the steal benchmarks leave at their defaults.
	 *
	 * @param cachePreparedStatements {@link PgConnectOptions#setCachePreparedStatements(boolean)}
	 * @param pipeliningLimit {@link PgConnectOptions#setPipeliningLimit(int)}, or 0 for the default of the client
	 * @param countSteals count the stolen connections ({@link StealCounter}), which costs a little CPU for each session
	 */
	public record ClientOptions(boolean cachePreparedStatements, int pipeliningLimit, boolean countSteals) {
		static final ClientOptions STEAL_BENCHMARK = new ClientOptions( false, 0, true );
	}

	final Vertx vertx;
	final StealCounter stealCounter;
	// The StealCounter proxy in front of the pool selected by poolType
	final Pool pool;
	final Mutiny.SessionFactory sessionFactory;
	private final ClientOptions clientOptions;

	private final String poolType;
	private final long localWaitMs;
	private final LocalWaitConfig localWait;
	private final PgConnectOptions connectOptions;
	private final PoolOptions poolOptions;

	/**
	 * @param poolType {@code shared} for one pool for all the event loops (like Quarkus),
	 * {@code perEventLoop} for one pool per event loop with {@code poolSize / eventLoopCount} connections each,
	 * {@code releasesFirst} for a shared pool that processes the releases of connections first ({@link ReleasesFirstExecutor}),
	 * {@code localWait} for a shared pool where a request waits a little for a connection of its event loop ({@link LocalWaitConfig}),
	 * or a {@link StealPolicyPool.Policy} in camel case ({@code always}, {@code alwaysActive}, {@code preferLocalWait},
	 * {@code never}) for a {@link StealPolicyPool}
	 * @param poolEventLoopSize when &gt; 0, spreads the connections of the shared pool across the event loops
	 * like Quarkus does ({@link PoolOptions#setEventLoopSize(int)})
	 * @param localWaitMs how long {@code preferLocalWait} waits for a connection of the same event loop
	 * @param localWait the settings of {@code localWait}, a shared pool that waits for a connection of the same
	 * event loop ({@code LocalWaitPool})
	 * @param nativeTransport use the native epoll transport instead of NIO, if it's on the classpath ({@code -Pepoll})
	 */
	ReactiveSetup(int eventLoopCount, String poolType, int poolSize, int poolEventLoopSize, long localWaitMs, LocalWaitConfig localWait,
			boolean nativeTransport) {
		this( eventLoopCount, poolType, poolSize, poolEventLoopSize, localWaitMs, localWait, nativeTransport, ClientOptions.STEAL_BENCHMARK );
	}

	/**
	 * A Quarkus-like pool ({@code poolEventLoopSize} = the number of event loops) of {@code shared},
	 * {@code localWait} (waiting {@code localWaitUs}) or {@code perEventLoop} connections.
	 */
	public ReactiveSetup(int eventLoopCount, String poolType, int poolSize, long localWaitUs, boolean nativeTransport,
			ClientOptions clientOptions) {
		this( eventLoopCount, poolType, poolSize, "shared".equals( poolType ) || "localWait".equals( poolType ) ? eventLoopCount : 0, 1, new LocalWaitConfig( localWaitUs, 0, true, new LocalWaitStats() ),
				nativeTransport, clientOptions );
	}

	ReactiveSetup(int eventLoopCount, String poolType, int poolSize, int poolEventLoopSize, long localWaitMs, LocalWaitConfig localWait,
			boolean nativeTransport, ClientOptions clientOptions) {
		this.clientOptions = clientOptions;
		this.poolType = poolType;
		this.localWaitMs = localWaitMs;
		this.localWait = localWait;
		vertx = Vertx.vertx( new VertxOptions().setEventLoopPoolSize( eventLoopCount ).setPreferNativeTransport( nativeTransport ) );

		connectOptions = new PgConnectOptions()
				.setHost( "localhost" )
				.setPort( 5432 )
				.setDatabase( "hibernate_orm_test" )
				.setUser( "hibernate_orm_test" )
				.setPassword( "hibernate_orm_test" )
				.setCachePreparedStatements( clientOptions.cachePreparedStatements() );
		if ( clientOptions.pipeliningLimit() > 0 ) {
			connectOptions.setPipeliningLimit( clientOptions.pipeliningLimit() );
		}
		poolOptions = new PoolOptions().setMaxSize( poolSize ).setEventLoopSize( poolEventLoopSize );
		stealCounter = new StealCounter( vertx, clientOptions.countSteals() );
		pool = stealCounter.wrap( createPool() );

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
				pool,
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
	}

	/**
	 * The connections opened so far (dialect detection, schema creation, {@link #populateWorlds()})
	 * belong to the event loops of the contexts Vert.x created for the setup thread, not to the verticles.
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
			case "releasesFirst" -> ReleasesFirstExecutor.createPool( vertx, connectOptions, poolOptions );
			case "localWait" -> LocalWaitPool.createPool( vertx, connectOptions, poolOptions, localWait );
			default -> StealPolicyPool.create( vertx, connectOptions, poolOptions, policy(), localWaitMs );
		};
	}

	private StealPolicyPool.Policy policy() {
		try {
			return StealPolicyPool.Policy.fromCamelCase( poolType );
		}
		catch (IllegalArgumentException e) {
			throw new IllegalArgumentException( "Unknown pool type: " + poolType
					+ " (valid: shared, perEventLoop, releasesFirst, localWait, always, alwaysActive, preferLocalWait, never)", e );
		}
	}

	private void populateWorlds() {
		final int batchSize = 500;
		for ( int batch = 0; batch < WORLD_COUNT / batchSize; batch++ ) {
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

	public Mutiny.SessionFactory sessionFactory() {
		return sessionFactory;
	}

	public Vertx vertx() {
		return vertx;
	}

	public boolean nativeTransport() {
		return vertx.isNativeTransportEnabled();
	}

	public void close() {
		sessionFactory.close();
		pool.close().toCompletionStage().toCompletableFuture().join();
		vertx.close().toCompletionStage().toCompletableFuture().join();
	}
}

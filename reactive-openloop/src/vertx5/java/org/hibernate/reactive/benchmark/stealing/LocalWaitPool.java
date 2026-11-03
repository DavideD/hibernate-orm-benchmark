/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.netty.channel.EventLoop;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.pool.ConnectionPool;
import io.vertx.core.internal.pool.LocalWaitConnectionPool;
import io.vertx.core.internal.pool.PoolConnector;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;

/**
 * The shared Vert.x pool (like Quarkus, {@code PoolOptions#setEventLoopSize}) where a request that finds no free
 * connection on its event loop waits a little for one, before it can take a connection of another event loop.
 * <p>
 * The pool is a normal one, created by {@link Pool#pool}, whose {@code SimpleConnectionPool} is replaced with
 * a {@link LocalWaitConnectionPool}, a fork of it. Everything else (idle eviction, maximum lifetime, connection
 * failures, the pipelining, the wait queue size) is still done by the Vert.x code around it.
 * It uses the {@code connector}, the maximum size, the maximum wait queue size, and the context provider
 * (round-robin on the event loops) of the replaced pool.
 * <p>
 * It depends on the internals of Vert.x 5. The copy in {@code src/vertx4/java} does the same for Vert.x 4.
 */
final class LocalWaitPool {

	private LocalWaitPool() {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static Pool createPool(Vertx vertx, SqlConnectOptions connectOptions, PoolOptions poolOptions, LocalWaitConfig config) {
		Pool pool = Pool.pool( vertx, connectOptions, poolOptions );
		try {
			Object sqlConnectionPool = PoolReflection.sqlConnectionPool( pool );
			Field poolField = PoolReflection.field( sqlConnectionPool.getClass(), "pool" );
			Object original = poolField.get( sqlConnectionPool );
			var connector = (PoolConnector) PoolReflection.field( sqlConnectionPool.getClass(), "connector" ).get( sqlConnectionPool );
			int maxWaiters = (int) PoolReflection.field( original.getClass(), "maxWaiters" ).get( original );
			var contextProvider = (Function<ContextInternal, ContextInternal>) PoolReflection.field( original.getClass(), "contextProvider" ).get( original );

			// The loops of the round-robin of the context provider: calling it once for each of them leaves it as it was.
			// It doesn't use its argument when the pool has more than 1 event loop.
			List<EventLoop> eventLoops = new ArrayList<>();
			for ( int i = 0; i < poolOptions.getEventLoopSize(); i++ ) {
				eventLoops.add( contextProvider.apply( null ).nettyEventLoop() );
			}

			var options = new LocalWaitConnectionPool.Options( TimeUnit.MICROSECONDS.toNanos( config.localWaitUs() ), config.depth(),
					config.creationAffinity(), eventLoops, LocalWaitConnectionPool.NETTY_SCHEDULER, config.stats() );
			ConnectionPool<?> replacement = new LocalWaitConnectionPool<>( connector, new int[] { poolOptions.getMaxSize() }, maxWaiters, options )
					.contextProvider( contextProvider );
			// The pool field is final, but it can be set before the pool is used
			poolField.set( sqlConnectionPool, replacement );
		}
		catch (ReflectiveOperationException e) {
			throw new IllegalStateException( "Can't replace the connection pool, the Vert.x internals have changed", e );
		}
		return pool;
	}
}

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import io.netty.channel.EventLoop;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;

/**
 * A {@link Pool} made of one plain pool per event loop, so that a connection is never stolen.
 * The connections are split evenly between the event loops: a busy loop can't use the idle connections of the others.
 * <p>
 * Based on the {@code PerEventLoopPool} by tsegismont in
 * <a href="https://github.com/DavideD/FrameworkBenchmarks/pull/1">DavideD/FrameworkBenchmarks#1</a>,
 * but implemented as a proxy so that it works with Vert.x 4 and 5.
 * <p>
 * With {@link PoolOptions#setEventLoopSize(int)} set to 0, a pool creates the connection on the event loop
 * of the context calling {@link Pool#getConnection()}. Each call goes to the pool of that event loop,
 * so every pool only has connections on its own loop, even when called from a thread that isn't an event loop.
 */
final class PerEventLoopPool {

	private PerEventLoopPool() {
	}

	static Pool create(Vertx vertx, SqlConnectOptions connectOptions, PoolOptions poolOptions) {
		List<EventLoop> eventLoops = VertxInternals.eventLoops( vertx );
		int eventLoopCount = eventLoops.size();
		if ( poolOptions.getMaxSize() < eventLoopCount ) {
			throw new IllegalArgumentException( "The pool size (" + poolOptions.getMaxSize()
					+ ") must be at least the number of event loops (" + eventLoopCount + ")" );
		}

		Map<EventLoop, Pool> pools = new IdentityHashMap<>();
		for ( int i = 0; i < eventLoopCount; i++ ) {
			// If the connections can't be split evenly, the extra ones go to the first pool
			int maxSize = poolOptions.getMaxSize() / eventLoopCount
					+ ( i == 0 ? poolOptions.getMaxSize() % eventLoopCount : 0 );
			var options = new PoolOptions( poolOptions ).setMaxSize( maxSize ).setEventLoopSize( 0 );
			pools.put( eventLoops.get( i ), Pool.pool( vertx, connectOptions, options ) );
		}

		return (Pool) Proxy.newProxyInstance(
				Pool.class.getClassLoader(),
				new Class<?>[] { Pool.class },
				(proxy, method, args) -> {
					if ( "close".equals( method.getName() ) && method.getParameterCount() == 0 ) {
						List<Future<Void>> closed = new ArrayList<>( pools.size() );
						pools.values().forEach( pool -> closed.add( pool.close() ) );
						return Future.join( closed ).mapEmpty();
					}
					// The context Vert.x uses for the call, created if the caller isn't on a Vert.x thread
					EventLoop eventLoop = VertxInternals.eventLoopOf( vertx.getOrCreateContext() );
					try {
						// size() returns the size of the current event loop pool
						return method.invoke( pools.get( eventLoop ), args );
					}
					catch (InvocationTargetException e) {
						throw e.getCause();
					}
				}
		);
	}
}

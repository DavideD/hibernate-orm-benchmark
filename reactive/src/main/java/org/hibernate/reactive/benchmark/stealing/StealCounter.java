/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import io.netty.channel.EventLoop;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;

/**
 * Wraps a {@link Pool} and counts, for each {@link Pool#getConnection()} called on an event loop,
 * whether the connection runs on the caller's event loop or on another one ("stolen").
 * <p>
 * The pool behind the proxy can be replaced with {@link #replace(Pool)}, so that the connections
 * opened during the setup don't skew the results.
 */
final class StealCounter {

	private final List<EventLoop> eventLoops;
	private final LongAdder local = new LongAdder();
	private final LongAdder stolen = new LongAdder();
	private final Map<EventLoop, LongAdder> requestsPerLoop = new ConcurrentHashMap<>();
	// The pool keeps its connections open, so the map doesn't grow past the pool size
	private final Map<Object, EventLoop> connections = Collections.synchronizedMap( new IdentityHashMap<>() );
	private volatile Pool delegate;

	StealCounter(Vertx vertx) {
		this.eventLoops = VertxInternals.eventLoops( vertx );
	}

	Pool wrap(Pool pool) {
		delegate = pool;
		return (Pool) Proxy.newProxyInstance(
				Pool.class.getClassLoader(),
				new Class<?>[] { Pool.class },
				(proxy, method, args) -> {
					try {
						Object result = method.invoke( delegate, args );
						if ( "getConnection".equals( method.getName() ) && method.getParameterCount() == 0 ) {
							return track( (Future<SqlConnection>) result );
						}
						return result;
					}
					catch (InvocationTargetException e) {
						throw e.getCause();
					}
				}
		);
	}

	/**
	 * Replace the pool behind the proxy.
	 *
	 * @return the previous pool, that the caller should close
	 */
	Pool replace(Pool pool) {
		Pool previous = delegate;
		delegate = pool;
		connections.clear();
		reset();
		return previous;
	}

	private Future<SqlConnection> track(Future<SqlConnection> future) {
		EventLoop callerLoop = VertxInternals.currentEventLoop();
		if ( callerLoop == null ) {
			return future;
		}
		requestsPerLoop.computeIfAbsent( callerLoop, loop -> new LongAdder() ).increment();
		return future.map( connection -> {
			EventLoop connectionLoop = VertxInternals.eventLoopOf( connection );
			connections.put( VertxInternals.socketOf( connection ), connectionLoop );
			if ( connectionLoop == callerLoop ) {
				local.increment();
			}
			else {
				stolen.increment();
			}
			return connection;
		} );
	}

	void reset() {
		local.reset();
		stolen.reset();
		requestsPerLoop.clear();
	}

	long acquired() {
		return local.sum() + stolen.sum();
	}

	long stolen() {
		return stolen.sum();
	}

	String report() {
		long l = local.sum();
		long s = stolen.sum();
		long total = l + s;
		var report = new StringBuilder( String.format(
				"connections acquired: %d, stolen: %d (%.1f%%)",
				total, s, total == 0 ? 0.0 : 100.0 * s / total
		) );
		Map<EventLoop, Long> connectionsPerLoop = new IdentityHashMap<>();
		synchronized ( connections ) {
			connections.values().forEach( loop -> connectionsPerLoop.merge( loop, 1L, Long::sum ) );
		}
		for ( int i = 0; i < eventLoops.size(); i++ ) {
			EventLoop loop = eventLoops.get( i );
			LongAdder requests = requestsPerLoop.get( loop );
			report.append( String.format(
					"%n  loop %d: %d connections, %d requests",
					i, connectionsPerLoop.getOrDefault( loop, 0L ), requests == null ? 0 : requests.sum()
			) );
		}
		return report.toString();
	}
}

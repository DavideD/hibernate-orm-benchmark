/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.LongAdder;

import io.netty.channel.EventLoop;
import io.vertx.core.Future;
import io.vertx.core.impl.ContextInternal;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.SocketConnectionBase;
import io.vertx.sqlclient.impl.SqlConnectionInternal;

/**
 * Wraps a {@link Pool} and counts, for each {@link Pool#getConnection()} called on an event loop,
 * whether the connection runs on the caller's event loop or on another one ("stolen").
 */
final class StealCounter {

	final LongAdder local = new LongAdder();
	final LongAdder stolen = new LongAdder();

	Pool wrap(Pool delegate) {
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

	private Future<SqlConnection> track(Future<SqlConnection> future) {
		ContextInternal caller = ContextInternal.current();
		if ( caller == null || !caller.isEventLoopContext() ) {
			return future;
		}
		EventLoop callerLoop = caller.nettyEventLoop();
		return future.map( connection -> {
			var socket = (SocketConnectionBase) ( (SqlConnectionInternal) connection ).unwrap().unwrap();
			if ( ( (ContextInternal) socket.context() ).nettyEventLoop() == callerLoop ) {
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
	}

	String report() {
		long l = local.sum();
		long s = stolen.sum();
		long total = l + s;
		return String.format( "connections acquired: %d, stolen: %d (%.1f%%)", total, s, total == 0 ? 0.0 : 100.0 * s / total );
	}
}

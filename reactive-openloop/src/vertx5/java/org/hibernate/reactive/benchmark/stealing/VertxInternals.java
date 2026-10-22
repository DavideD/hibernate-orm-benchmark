/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutor;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.VertxInternal;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.SocketConnectionBase;
import io.vertx.sqlclient.internal.SqlConnectionInternal;

/**
 * Access to the Vert.x 5 internals needed by {@link StealCounter}, {@link PerEventLoopPool} and {@link StealPolicyPool}.
 * Vert.x 4 has these classes in other packages, see the copy in {@code src/vertx4/java}.
 */
final class VertxInternals {

	private VertxInternals() {
	}

	/**
	 * @return the event loop of the current context, or {@code null} if not running on an event loop context
	 */
	static EventLoop currentEventLoop() {
		ContextInternal context = ContextInternal.current();
		return context != null && context.isEventLoopContext() ? context.nettyEventLoop() : null;
	}

	/**
	 * @return the event loop of a context
	 */
	static EventLoop eventLoopOf(Context context) {
		return ( (ContextInternal) context ).nettyEventLoop();
	}

	/**
	 * @return the socket connection of a connection obtained from a pool.
	 * The pool wraps it in a new object each time, the socket stays the same.
	 */
	static Object socketOf(SqlConnection connection) {
		return ( (SqlConnectionInternal) connection ).unwrap().unwrap();
	}

	/**
	 * @return the event loop that owns the socket of a connection obtained from a pool
	 */
	static EventLoop eventLoopOf(SqlConnection connection) {
		var socket = (SocketConnectionBase) socketOf( connection );
		return eventLoopOf( socket.context() );
	}

	/**
	 * @return a new context running on the given event loop
	 */
	static Context createContext(Vertx vertx, EventLoop eventLoop) {
		return ( (VertxInternal) vertx ).contextBuilder()
				.withEventLoop( eventLoop )
				.withClassLoader( Thread.currentThread().getContextClassLoader() )
				.build();
	}

	static List<EventLoop> eventLoops(Vertx vertx) {
		List<EventLoop> loops = new ArrayList<>();
		for ( EventExecutor executor : ( (VertxInternal) vertx ).nettyEventLoopGroup() ) {
			loops.add( (EventLoop) executor );
		}
		return loops;
	}
}

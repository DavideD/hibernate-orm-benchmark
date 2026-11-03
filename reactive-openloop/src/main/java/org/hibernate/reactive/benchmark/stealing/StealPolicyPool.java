/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.EventLoop;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.SqlConnection;

/**
 * A prototype of a {@link Pool} where a {@link Policy} decides when a request can use a connection of another event loop.
 * <p>
 * Vert.x doesn't let us change this in its own pool: when a connection is released, it always goes to the request
 * that has waited the longest, whatever its event loop. So this pool is made of one Vert.x pool per event loop,
 * and decides itself which of them each request gets its connection from:
 * <ul>
 *     <li>All the connections are opened at the start, on the event loop of their pool,
 *     so every connection belongs to a known event loop. The Vert.x pool opens them when they're needed instead,
 *     but a busy pool is full anyway.</li>
 *     <li>It counts the free connections of each event loop, and keeps the requests that wait for a connection
 *     in a queue for each event loop, and a queue for those that accept a connection from any event loop.</li>
 *     <li>A connection from another event loop is still acquired on the event loop of the request,
 *     so Vert.x sends the results back to it, like it does when its own pool hands out such a connection.</li>
 *     <li>The connections are wrapped to know when they're released.</li>
 * </ul>
 * The connections are split between the event loops like in {@link PerEventLoopPool}.
 */
final class StealPolicyPool {

	/**
	 * What a request does when its event loop has no free connection.
	 * Based on the options proposed in the discussion about connection stealing.
	 */
	enum Policy {
		/**
		 * Take a free connection of another event loop.
		 * If there's none, wait for the first connection released by any event loop. That's what the Vert.x pool does.
		 */
		ALWAYS,
		/**
		 * Take a free connection of another event loop, already open.
		 * If there's none, wait for a connection of the same event loop: never get one released by another event loop.
		 */
		ALWAYS_ACTIVE,
		/**
		 * Wait for a connection of the same event loop, up to {@code localWaitMs}.
		 * Then do like {@link #ALWAYS}.
		 */
		PREFER_LOCAL_WAIT,
		/**
		 * Only use connections of the same event loop: wait for one if there's none.
		 */
		NEVER;

		/**
		 * @param name the name in camel case, like {@code preferLocalWait}
		 */
		static Policy fromCamelCase(String name) {
			return valueOf( name.replaceAll( "([a-z])([A-Z])", "$1_$2" ).toUpperCase( Locale.ROOT ) );
		}
	}

	private StealPolicyPool() {
	}

	/**
	 * @param localWaitMs how long {@link Policy#PREFER_LOCAL_WAIT} waits for a connection of the same event loop,
	 * at least 1 ms (the resolution of the Vert.x timers)
	 */
	static Pool create(Vertx vertx, SqlConnectOptions connectOptions, PoolOptions poolOptions, Policy policy, long localWaitMs) {
		if ( policy == Policy.PREFER_LOCAL_WAIT && localWaitMs < 1 ) {
			throw new IllegalArgumentException( "localWaitMs must be at least 1: " + localWaitMs );
		}
		var coordinator = new Coordinator( vertx, connectOptions, poolOptions, policy, localWaitMs );
		return (Pool) Proxy.newProxyInstance(
				Pool.class.getClassLoader(),
				new Class<?>[] { Pool.class },
				(proxy, method, args) -> {
					if ( method.isDefault() ) {
						// withConnection(), withTransaction(): they call getConnection() on the proxy
						return InvocationHandler.invokeDefault( proxy, method, args );
					}
					switch ( method.getName() ) {
						case "getConnection":
							if ( method.getParameterCount() == 0 ) {
								return coordinator.acquire();
							}
							// Vert.x 4 getConnection(Handler)
							coordinator.acquire().onComplete( (Handler<AsyncResult<SqlConnection>>) args[0] );
							return null;
						case "close":
							if ( method.getParameterCount() == 0 ) {
								return coordinator.close();
							}
							coordinator.close().onComplete( (Handler<AsyncResult<Void>>) args[0] );
							return null;
						case "size":
							return coordinator.size();
						case "toString":
							return "StealPolicyPool(" + policy + ")";
						case "hashCode":
							return System.identityHashCode( proxy );
						case "equals":
							return proxy == args[0];
						default:
							// Queries on the pool itself would take connections without going through the policy
							throw new UnsupportedOperationException( "StealPolicyPool doesn't support Pool#" + method.getName() );
					}
				}
		);
	}

	private static final class Waiter {
		final int eventLoop;
		final Context context;
		final Promise<SqlConnection> promise = Promise.promise();
		// Guarded by the coordinator lock
		boolean served;
		long timerId = -1;

		Waiter(int eventLoop, Context context) {
			this.eventLoop = eventLoop;
			this.context = context;
		}
	}

	private static final class Coordinator {

		private final Vertx vertx;
		private final Policy policy;
		private final long localWaitMs;
		private final Map<EventLoop, Integer> eventLoopIndexes = new IdentityHashMap<>();
		private final Pool[] pools;

		// All guarded by the lock
		private final Object lock = new Object();
		private final int[] free;
		private final ArrayDeque<Waiter>[] localWaiters;
		private final ArrayDeque<Waiter> anyWaiters = new ArrayDeque<>();

		@SuppressWarnings("unchecked")
		Coordinator(Vertx vertx, SqlConnectOptions connectOptions, PoolOptions poolOptions, Policy policy, long localWaitMs) {
			this.vertx = vertx;
			this.policy = policy;
			this.localWaitMs = localWaitMs;
			List<EventLoop> eventLoops = VertxInternals.eventLoops( vertx );
			int count = eventLoops.size();
			if ( poolOptions.getMaxSize() < count ) {
				throw new IllegalArgumentException( "The pool size (" + poolOptions.getMaxSize()
						+ ") must be at least the number of event loops (" + count + ")" );
			}
			pools = new Pool[count];
			free = new int[count];
			localWaiters = new ArrayDeque[count];
			for ( int i = 0; i < count; i++ ) {
				eventLoopIndexes.put( eventLoops.get( i ), i );
				// If the connections can't be split evenly, the extra ones go to the first pool
				int maxSize = poolOptions.getMaxSize() / count + ( i == 0 ? poolOptions.getMaxSize() % count : 0 );
				// With eventLoopSize 0, a connection is opened on the event loop that asks for it
				var options = new PoolOptions( poolOptions ).setMaxSize( maxSize ).setEventLoopSize( 0 );
				pools[i] = Pool.pool( vertx, connectOptions, options );
				free[i] = maxSize;
				localWaiters[i] = new ArrayDeque<>();
				open( pools[i], maxSize, VertxInternals.createContext( vertx, eventLoops.get( i ) ) );
			}
		}

		/**
		 * Open all the connections of a pool on the event loop of the context, then give them back to the pool.
		 * The pool is then full: when another event loop asks for a connection, Vert.x hands out one of these,
		 * instead of opening a new one on the event loop that asks.
		 */
		private static void open(Pool pool, int size, Context context) {
			var opened = new CompletableFuture<Void>();
			context.runOnContext( v -> {
				List<Future<SqlConnection>> connections = new ArrayList<>( size );
				for ( int i = 0; i < size; i++ ) {
					connections.add( pool.getConnection() );
				}
				Future.all( connections )
						.compose( all -> {
							List<Future<Void>> closed = new ArrayList<>( size );
							connections.forEach( connection -> closed.add( connection.result().close() ) );
							return Future.all( closed );
						} )
						.onSuccess( all -> opened.complete( null ) )
						.onFailure( opened::completeExceptionally );
			} );
			opened.join();
		}

		Future<SqlConnection> acquire() {
			// The context Vert.x uses for the request, created if the caller isn't on a Vert.x thread
			Context context = vertx.getOrCreateContext();
			int local = eventLoopIndexes.get( VertxInternals.eventLoopOf( context ) );
			Waiter waiter;
			int from;
			synchronized ( lock ) {
				if ( free[local] > 0 ) {
					free[local]--;
					from = local;
				}
				else if ( ( policy == Policy.ALWAYS || policy == Policy.ALWAYS_ACTIVE ) && ( from = takeFreeElsewhere( local ) ) >= 0 ) {
					// Stolen
				}
				else {
					from = -1;
				}
				if ( from >= 0 ) {
					waiter = null;
				}
				else {
					waiter = new Waiter( local, context );
					if ( policy == Policy.ALWAYS ) {
						anyWaiters.add( waiter );
					}
					else {
						localWaiters[local].add( waiter );
					}
				}
			}
			if ( waiter == null ) {
				return lease( from );
			}
			if ( policy == Policy.PREFER_LOCAL_WAIT ) {
				long timerId = vertx.setTimer( localWaitMs, id -> stopWaitingLocally( waiter ) );
				synchronized ( lock ) {
					if ( !waiter.served ) {
						waiter.timerId = timerId;
					}
				}
			}
			return waiter.promise.future();
		}

		/**
		 * {@link Policy#PREFER_LOCAL_WAIT}: no connection of the same event loop got free in time
		 */
		private void stopWaitingLocally(Waiter waiter) {
			int from;
			synchronized ( lock ) {
				if ( waiter.served ) {
					return;
				}
				localWaiters[waiter.eventLoop].remove( waiter );
				from = takeFreeElsewhere( waiter.eventLoop );
				if ( from < 0 ) {
					anyWaiters.add( waiter );
					return;
				}
				waiter.served = true;
			}
			serve( waiter, from );
		}

		// Must hold the lock
		private int takeFreeElsewhere(int local) {
			for ( int i = 1; i < free.length; i++ ) {
				int other = ( local + i ) % free.length;
				if ( free[other] > 0 ) {
					free[other]--;
					return other;
				}
			}
			return -1;
		}

		/**
		 * A connection of the event loop {@code from} has been released: give it to a request that waits for it,
		 * first one of the same event loop, then one that accepts any event loop.
		 */
		private void release(int from) {
			Waiter waiter;
			synchronized ( lock ) {
				waiter = localWaiters[from].poll();
				if ( waiter == null ) {
					waiter = anyWaiters.poll();
				}
				if ( waiter == null ) {
					free[from]++;
					return;
				}
				waiter.served = true;
			}
			serve( waiter, from );
		}

		private void serve(Waiter waiter, int from) {
			if ( waiter.timerId >= 0 ) {
				vertx.cancelTimer( waiter.timerId );
			}
			// Acquire it on the context of the request, so that Vert.x sends the results back to its event loop
			waiter.context.runOnContext( v -> lease( from ).onComplete( waiter.promise ) );
		}

		/**
		 * Get a connection of the pool of the event loop {@code from}, on the current context.
		 * The pool might not have recycled the released connection yet: then it waits for it.
		 */
		private Future<SqlConnection> lease(int from) {
			return pools[from].getConnection().map( connection -> wrap( connection, from ) );
		}

		/**
		 * A proxy of the connection that calls {@link #release(int)} when the connection is closed (given back to the pool).
		 * It has all the interfaces of the connection, so code that casts it to a Vert.x internal interface still works.
		 */
		private SqlConnection wrap(SqlConnection connection, int from) {
			var released = new AtomicBoolean();
			return (SqlConnection) Proxy.newProxyInstance(
					SqlConnection.class.getClassLoader(),
					publicInterfaces( connection.getClass() ),
					(proxy, method, args) -> {
						if ( "close".equals( method.getName() ) && method.getParameterCount() <= 1 ) {
							Future<Void> closed = connection.close();
							if ( released.compareAndSet( false, true ) ) {
								closed.onComplete( ar -> release( from ) );
							}
							if ( method.getParameterCount() == 1 ) {
								// Vert.x 4 close(Handler)
								closed.onComplete( (Handler<AsyncResult<Void>>) args[0] );
								return null;
							}
							return closed;
						}
						try {
							Object result = method.invoke( connection, args );
							// Fluent methods return the connection
							return result == connection ? proxy : result;
						}
						catch (InvocationTargetException e) {
							throw e.getCause();
						}
					}
			);
		}

		private static Class<?>[] publicInterfaces(Class<?> type) {
			Set<Class<?>> interfaces = new LinkedHashSet<>();
			for ( Class<?> current = type; current != null; current = current.getSuperclass() ) {
				for ( Class<?> anInterface : current.getInterfaces() ) {
					if ( Modifier.isPublic( anInterface.getModifiers() ) ) {
						interfaces.add( anInterface );
					}
				}
			}
			return interfaces.toArray( new Class<?>[0] );
		}

		Future<Void> close() {
			List<Future<Void>> closed = new ArrayList<>( pools.length );
			for ( Pool pool : pools ) {
				closed.add( pool.close() );
			}
			return Future.join( closed ).mapEmpty();
		}

		int size() {
			int size = 0;
			for ( Pool pool : pools ) {
				size += pool.size();
			}
			return size;
		}
	}
}

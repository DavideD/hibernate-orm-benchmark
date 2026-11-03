/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.EventLoop;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.net.impl.pool.ConnectResult;
import io.vertx.core.net.impl.pool.Lease;
import io.vertx.core.net.impl.pool.LocalWaitConnectionPool;
import io.vertx.core.net.impl.pool.PoolConnector;
import io.vertx.core.net.impl.pool.PoolWaiter;

/**
 * A {@link LocalWaitConnectionPool} with fake connections and timers that only fire when the test says so,
 * so that the tests don't depend on the database or on the time.
 * This is the Vert.x 4 version, the one in {@code src/testVertx5/java} has the same methods for Vert.x 5.
 * <p>
 * The pool runs its actions on the thread that calls it, and Vert.x delivers the connections to the event loop
 * of the connection: {@link #drain()} waits for that.
 */
final class LocalWaitPoolHarness implements AutoCloseable {

	/**
	 * A fake connection, on one event loop.
	 */
	record FakeConnection(int loop, int id) {
	}

	/**
	 * A timer started by the pool.
	 */
	static final class Timer {
		final int loop;
		final long delayNanos;
		final Runnable task;
		volatile boolean cancelled;
		volatile boolean fired;

		private Timer(int loop, long delayNanos, Runnable task) {
			this.loop = loop;
			this.delayNanos = delayNanos;
			this.task = task;
		}
	}

	/**
	 * A request for a connection, that gets one at most once.
	 */
	final class Request {
		final int loop;
		private final AtomicInteger served = new AtomicInteger();
		private volatile Lease<FakeConnection> lease;
		private volatile PoolWaiter<FakeConnection> waiter;
		private volatile Throwable failure;
		private Timer timer;
		private boolean released;

		private Request(int loop) {
			this.loop = loop;
		}

		/**
		 * @return how many times the pool gave a connection to this request
		 */
		int served() {
			return served.get();
		}

		boolean isServed() {
			return served.get() > 0;
		}

		/**
		 * @return why the pool couldn't give a connection to this request, or null
		 */
		Throwable failure() {
			return failure;
		}

		FakeConnection connection() {
			return lease.get();
		}

		/**
		 * @return the timer of the local wait of this request, or null if it didn't wait for a local connection
		 */
		Timer timer() {
			return timer;
		}

		void release() {
			if ( released ) {
				throw new IllegalStateException( "Already released" );
			}
			released = true;
			lease.recycle();
			drain();
		}

		void cancel() {
			pool.cancel( waiter, result -> {
			} );
			drain();
		}
	}

	final Vertx vertx;
	final List<EventLoop> loops = new ArrayList<>();
	private final List<ContextInternal> contexts = new ArrayList<>();
	final List<Timer> timers = new CopyOnWriteArrayList<>();
	final LocalWaitStats stats = new LocalWaitStats();
	final LocalWaitConnectionPool<FakeConnection> pool;
	private final AtomicInteger connectionIds = new AtomicInteger();
	private final java.util.Map<Integer, PoolConnector.Listener> listeners = new java.util.concurrent.ConcurrentHashMap<>();
	private volatile boolean failNextConnect;

	/**
	 * @param maxSize the maximum number of connections
	 * @param eventLoops the number of event loops
	 * @param localWaitUs how long a request waits for a connection of its event loop
	 * @param depth the depth of the queue that skips the local wait
	 * @param creationAffinity whether the connections are created on the event loop that needs them
	 * @param roundRobin whether the connections that don't use the affinity go to the event loops in turn
	 * (like with {@code PoolOptions#setEventLoopSize}), or to the event loop of the request
	 */
	LocalWaitPoolHarness(int maxSize, int eventLoops, long localWaitUs, int depth, boolean creationAffinity, boolean roundRobin) {
		vertx = Vertx.vertx( new VertxOptions().setEventLoopPoolSize( eventLoops ) );
		loops.addAll( VertxInternals.eventLoops( vertx ) );
		for ( EventLoop loop : loops ) {
			contexts.add( (ContextInternal) VertxInternals.createContext( vertx, loop ) );
		}
		var options = new LocalWaitConnectionPool.Options( localWaitUs * 1000, depth, creationAffinity,
				List.copyOf( loops ), this::schedule, stats );
		pool = new LocalWaitConnectionPool<>( new FakeConnector(), new int[] { maxSize }, -1, options );
		if ( roundRobin ) {
			var next = new AtomicInteger();
			pool.contextProvider( requester -> contexts.get( next.getAndIncrement() % loops.size() ) );
		}
	}

	private Runnable schedule(EventLoop eventLoop, long delayNanos, Runnable task) {
		var timer = new Timer( loops.indexOf( eventLoop ), delayNanos, task );
		timers.add( timer );
		return () -> timer.cancelled = true;
	}

	private final class FakeConnector implements PoolConnector<FakeConnection> {
		@Override
		public void connect(ContextInternal context, Listener listener, Handler<AsyncResult<ConnectResult<FakeConnection>>> handler) {
			if ( failNextConnect ) {
				failNextConnect = false;
				handler.handle( Future.failedFuture( new IllegalStateException( "Can't connect" ) ) );
				return;
			}
			var connection = new FakeConnection( loops.indexOf( context.nettyEventLoop() ), connectionIds.getAndIncrement() );
			listeners.put( connection.id(), listener );
			handler.handle( Future.succeededFuture( new ConnectResult<>( connection, 1, 0 ) ) );
		}

		@Override
		public boolean isValid(FakeConnection connection) {
			return true;
		}
	}

	/**
	 * A request from an event loop, that holds the connection until the test releases it.
	 */
	Request acquire(int loop) {
		var request = new Request( loop );
		int timersBefore = timers.size();
		pool.acquire( contexts.get( loop ), new PoolWaiter.Listener<>() {
			@Override
			public void onEnqueue(PoolWaiter<FakeConnection> waiter) {
				request.waiter = waiter;
			}

			@Override
			public void onConnect(PoolWaiter<FakeConnection> waiter) {
				request.waiter = waiter;
			}
		}, 0, result -> {
			if ( result.failed() ) {
				request.failure = result.cause();
			}
			else {
				request.lease = result.result();
				request.served.incrementAndGet();
			}
		} );
		drain();
		if ( timers.size() > timersBefore ) {
			request.timer = timers.get( timers.size() - 1 );
		}
		return request;
	}

	/**
	 * The next connection the pool opens fails.
	 */
	void failNextConnect() {
		failNextConnect = true;
	}

	/**
	 * The connection is closed: it tells the pool to remove it, like the connections of the SQL client do.
	 */
	void close(FakeConnection connection) {
		listeners.get( connection.id() ).onRemove();
		drain();
	}

	/**
	 * Evict the connections that aren't in use, like the idle timeout and the maximum lifetime do.
	 *
	 * @return the connections that were evicted
	 */
	List<FakeConnection> evict() {
		List<FakeConnection> evicted = new ArrayList<>();
		pool.evict( connection -> true, result -> evicted.addAll( result.result() ) );
		drain();
		return evicted;
	}

	/**
	 * Run the timers that haven't been cancelled, as the event loops would.
	 */
	void fireDeadlines() {
		for ( Timer timer : timers ) {
			if ( !timer.cancelled && !timer.fired ) {
				fire( timer );
			}
		}
	}

	/**
	 * Run the timer, even if the pool cancelled it: it could have been already running.
	 */
	void fire(Timer timer) {
		timer.fired = true;
		timer.task.run();
		drain();
	}

	/**
	 * Wait until the event loops have delivered the connections to the requests.
	 */
	void drain() {
		try {
			for ( EventLoop loop : loops ) {
				loop.submit( () -> {
				} ).get();
			}
		}
		catch (Exception e) {
			throw new IllegalStateException( e );
		}
	}

	@Override
	public void close() {
		vertx.close().toCompletionStage().toCompletableFuture().join();
	}
}

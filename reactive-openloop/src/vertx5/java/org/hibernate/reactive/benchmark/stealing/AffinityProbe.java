/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

import io.netty.channel.EventLoop;
import io.vertx.core.Vertx;
import io.vertx.core.internal.pool.Executor;
import io.vertx.core.internal.pool.PoolConnection;
import io.vertx.core.internal.pool.PoolWaiter;
import io.vertx.core.internal.pool.Task;
import io.vertx.sqlclient.Pool;

import org.HdrHistogram.Histogram;

/**
 * Instruments a Vert.x pool to count the acquisitions that miss a connection of their own event loop
 * because its release hasn't been processed yet.
 * <p>
 * The Vert.x pool ({@code SimpleConnectionPool}) processes all its actions (acquire, release, ...) in the order
 * they're submitted, from a single queue. When an acquisition finds no free connection on its event loop,
 * it takes one from another event loop or waits, even if the release of a connection of its event loop
 * has already been submitted, behind it in the queue. This probe counts those "missed" acquisitions,
 * and measures how long after such an acquisition the pool processes the release of a connection of its event loop.
 * <p>
 * It replaces the private executor of the pool with one that wraps the actions: it depends on the internals of
 * Vert.x 5. The copy in {@code src/vertx4/java} does the same for Vert.x 4.
 */
final class AffinityProbe {

	private final Map<EventLoop, Integer> eventLoopIndexes = new IdentityHashMap<>();
	// Releases submitted to the pool but not processed yet, for each event loop
	private final AtomicLongArray pendingReleases;
	private final Field slotField;

	// The pool runs one action at a time, but not always on the same thread: guarded by the lock
	private final Object lock = new Object();
	private final ArrayDeque<Long>[] noLocalSince;
	private long acquisitions;
	private long noLocalFree;
	private long missed;
	private Histogram timeToLocalRelease = newHistogram();

	@SuppressWarnings("unchecked")
	private AffinityProbe(Vertx vertx, Field slotField) {
		List<EventLoop> eventLoops = VertxInternals.eventLoops( vertx );
		for ( int i = 0; i < eventLoops.size(); i++ ) {
			eventLoopIndexes.put( eventLoops.get( i ), i );
		}
		this.pendingReleases = new AtomicLongArray( eventLoops.size() );
		this.noLocalSince = new ArrayDeque[eventLoops.size()];
		for ( int i = 0; i < noLocalSince.length; i++ ) {
			noLocalSince[i] = new ArrayDeque<>();
		}
		this.slotField = slotField;
	}

	/**
	 * @param pool a pool created with {@link Pool#pool}, not a proxy
	 */
	@SuppressWarnings("unchecked")
	static AffinityProbe install(Pool pool, Vertx vertx) {
		try {
			Object connectionPool = PoolReflection.connectionPool( pool );
			Field syncField = PoolReflection.executorField( connectionPool );
			Field slotsField = PoolReflection.field( connectionPool.getClass(), "slots" );
			Field sizeField = PoolReflection.field( connectionPool.getClass(), "size" );
			Executor<Object> sync = (Executor<Object>) syncField.get( connectionPool );
			Field slotField = PoolReflection.field( Class.forName( connectionPool.getClass().getName() + "$Recycle" ), "slot" );

			var probe = new AffinityProbe( vertx, slotField );
			Executor<Object> instrumented = action -> probe.submit( sync, action, slotsField, sizeField );
			syncField.set( connectionPool, instrumented );
			return probe;
		}
		catch (ReflectiveOperationException e) {
			throw new IllegalStateException( "Can't instrument the pool, the Vert.x internals have changed", e );
		}
	}

	private void submit(Executor<Object> sync, Executor.Action<Object> action, Field slotsField, Field sizeField) {
		switch ( action.getClass().getSimpleName() ) {
			case "Recycle" -> {
				int eventLoop = indexOf( ( (PoolConnection<?>) PoolReflection.read( action, slotField ) ).context().nettyEventLoop() );
				pendingReleases.incrementAndGet( eventLoop );
				sync.submit( new Release( action, eventLoop ) );
			}
			case "Acquire" -> {
				int eventLoop = indexOf( ( (PoolWaiter<?>) action ).context().nettyEventLoop() );
				sync.submit( state -> {
					acquiring( eventLoop, (Object[]) PoolReflection.read( state, slotsField ), (int) PoolReflection.read( state, sizeField ) );
					return action.execute( state );
				} );
			}
			default -> sync.submit( action );
		}
	}

	/**
	 * A release, wrapped to count it. A class, not a lambda, so that {@link ReleasesFirstExecutor} can recognize it.
	 */
	final class Release implements Executor.Action<Object> {
		private final Executor.Action<Object> recycle;
		private final int eventLoop;

		private Release(Executor.Action<Object> recycle, int eventLoop) {
			this.recycle = recycle;
			this.eventLoop = eventLoop;
		}

		@Override
		public Task execute(Object state) {
			pendingReleases.decrementAndGet( eventLoop );
			released( eventLoop );
			return recycle.execute( state );
		}
	}

	// Called by the pool, before it processes the acquisition
	private void acquiring(int eventLoop, Object[] slots, int size) {
		boolean localFree = false;
		for ( int i = 0; i < size && !localFree; i++ ) {
			var connection = (PoolConnection<?>) slots[i];
			localFree = connection.available() > 0 && indexOf( connection.context().nettyEventLoop() ) == eventLoop;
		}
		synchronized ( lock ) {
			acquisitions++;
			if ( !localFree ) {
				noLocalFree++;
				if ( pendingReleases.get( eventLoop ) > 0 ) {
					missed++;
				}
				noLocalSince[eventLoop].add( System.nanoTime() );
			}
		}
	}

	// Called by the pool, before it processes the release
	private void released(int eventLoop) {
		long now = System.nanoTime();
		synchronized ( lock ) {
			Long since;
			while ( ( since = noLocalSince[eventLoop].poll() ) != null ) {
				timeToLocalRelease.recordValue( Math.min( now - since, timeToLocalRelease.getHighestTrackableValue() ) );
			}
		}
	}

	void reset() {
		synchronized ( lock ) {
			acquisitions = 0;
			noLocalFree = 0;
			missed = 0;
			for ( var queue : noLocalSince ) {
				queue.clear();
			}
			timeToLocalRelease = newHistogram();
		}
	}

	Results results() {
		synchronized ( lock ) {
			return new Results( acquisitions, noLocalFree, missed, timeToLocalRelease.copy() );
		}
	}

	/**
	 * @param noLocalFree acquisitions that found no free connection on their event loop
	 * @param missed acquisitions that found no free connection on their event loop,
	 * while the release of a connection of their event loop was waiting to be processed
	 * @param timeToLocalRelease for the acquisitions that found no free connection on their event loop,
	 * the time until the pool processed the release of a connection of their event loop, in ns
	 */
	record Results(long acquisitions, long noLocalFree, long missed, Histogram timeToLocalRelease) {
	}

	private int indexOf(EventLoop eventLoop) {
		return eventLoopIndexes.get( eventLoop );
	}

	private static Histogram newHistogram() {
		return new Histogram( TimeUnit.SECONDS.toNanos( 10 ), 3 );
	}
}

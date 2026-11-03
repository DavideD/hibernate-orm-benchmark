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
import io.vertx.core.net.impl.pool.Executor;
import io.vertx.core.net.impl.pool.PoolConnection;
import io.vertx.core.net.impl.pool.PoolWaiter;
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
 * Vert.x 4. The copy in {@code src/vertx5/java} does the same for Vert.x 5.
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
			Object sqlConnectionPool = read( unwrap( pool ), "pool" );
			Object connectionPool = read( sqlConnectionPool, "pool" );
			Field syncField = field( connectionPool.getClass(), "sync" );
			Field slotsField = field( connectionPool.getClass(), "slots" );
			Field sizeField = field( connectionPool.getClass(), "size" );
			Executor<Object> sync = (Executor<Object>) syncField.get( connectionPool );
			Field slotField = field( Class.forName( connectionPool.getClass().getName() + "$Recycle" ), "slot" );

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
				int eventLoop = indexOf( ( (PoolConnection<?>) read( action, slotField ) ).context().nettyEventLoop() );
				pendingReleases.incrementAndGet( eventLoop );
				sync.submit( state -> {
					pendingReleases.decrementAndGet( eventLoop );
					released( eventLoop );
					return action.execute( state );
				} );
			}
			case "Acquire" -> {
				int eventLoop = indexOf( ( (PoolWaiter<?>) action ).context().nettyEventLoop() );
				sync.submit( state -> {
					acquiring( eventLoop, (Object[]) read( state, slotsField ), (int) read( state, sizeField ) );
					return action.execute( state );
				} );
			}
			default -> sync.submit( action );
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

	private static Object read(Object target, Field field) {
		try {
			return field.get( target );
		}
		catch (IllegalAccessException e) {
			throw new IllegalStateException( e );
		}
	}

	/**
	 * The pool can be a wrapper (PgPoolImpl in Vert.x 4) of the pool implementation (PoolImpl)
	 */
	private static Object unwrap(Pool pool) throws IllegalAccessException {
		Object current = pool;
		Field delegate;
		while ( ( delegate = fieldOrNull( current.getClass(), "delegate" ) ) != null ) {
			current = delegate.get( current );
		}
		return current;
	}

	private static Object read(Object target, String name) throws NoSuchFieldException, IllegalAccessException {
		return field( target.getClass(), name ).get( target );
	}

	private static Field field(Class<?> type, String name) throws NoSuchFieldException {
		Field field = fieldOrNull( type, name );
		if ( field == null ) {
			throw new NoSuchFieldException( type.getName() + "." + name );
		}
		return field;
	}

	private static Field fieldOrNull(Class<?> type, String name) {
		for ( Class<?> current = type; current != null; current = current.getSuperclass() ) {
			try {
				Field field = current.getDeclaredField( name );
				field.setAccessible( true );
				return field;
			}
			catch (NoSuchFieldException e) {
				// Try the superclass
			}
		}
		return null;
	}
}

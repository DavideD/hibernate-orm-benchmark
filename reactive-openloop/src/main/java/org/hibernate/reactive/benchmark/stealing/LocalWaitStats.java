/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * What happened to the acquisitions of the {@code LocalWaitConnectionPool} that found no free connection
 * on their event loop. Thread-safe: the pool updates it from the threads that run its actions.
 * It's in the main source set, the pool is in the version specific ones.
 */
public final class LocalWaitStats {

	/** Acquisitions that waited for a connection of their own event loop. */
	public final LongAdder localWaits = new LongAdder();
	/** Acquisitions that didn't wait for a local connection, because the event loop had too many waiters. */
	public final LongAdder depthShortcuts = new LongAdder();
	/** Acquisitions that didn't wait for a local connection, because their event loop had no connection. */
	public final LongAdder noLocalConnection = new LongAdder();
	/** Local waiters served by the release of a connection of their event loop. */
	public final LongAdder servedLocal = new LongAdder();
	/** Waiters that took a free connection of another event loop when their local wait ended. */
	public final LongAdder expiredStolen = new LongAdder();
	/** Waiters that found nothing free when their local wait ended, and joined the queue of the other waiters. */
	public final LongAdder expiredQueued = new LongAdder();
	/** Waiters that were not waiting for a local connection (any more), served by a released connection. */
	public final LongAdder servedAfterExpiry = new LongAdder();
	/** Connections opened on the event loop that needed them, instead of the next one in the round-robin. */
	public final LongAdder affinityCreations = new LongAdder();

	// Time from the start of the wait until the local waiter was served, in ns
	private final Recorder localWait = new Recorder( TimeUnit.SECONDS.toNanos( 10 ), 3 );
	// How late the local wait ended, compared to the deadline, in ns
	private final Recorder expiryLateness = new Recorder( TimeUnit.SECONDS.toNanos( 10 ), 3 );
	private Histogram recycled;
	private Histogram recycledLateness;

	public void recordServedLocal(long waitNanos) {
		servedLocal.increment();
		localWait.recordValue( clamp( waitNanos ) );
	}

	public void recordExpiry(long latenessNanos) {
		expiryLateness.recordValue( clamp( latenessNanos ) );
	}

	public synchronized void reset() {
		for ( LongAdder counter : new LongAdder[] { localWaits, depthShortcuts, noLocalConnection, servedLocal,
				expiredStolen, expiredQueued, servedAfterExpiry, affinityCreations } ) {
			counter.reset();
		}
		localWait.reset();
		expiryLateness.reset();
	}

	/**
	 * @return the time from the start of the wait until a local waiter was served by a release, in ns
	 */
	public synchronized Histogram localWaitHistogram() {
		recycled = localWait.getIntervalHistogram( recycled );
		return recycled.copy();
	}

	/**
	 * @return how late the local wait ended compared to its deadline, in ns
	 */
	public synchronized Histogram expiryLatenessHistogram() {
		recycledLateness = expiryLateness.getIntervalHistogram( recycledLateness );
		return recycledLateness.copy();
	}

	private static long clamp(long nanos) {
		return Math.max( 0, Math.min( nanos, TimeUnit.SECONDS.toNanos( 10 ) ) );
	}
}

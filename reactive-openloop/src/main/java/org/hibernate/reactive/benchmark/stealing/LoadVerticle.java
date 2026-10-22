/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.hibernate.reactive.mutiny.Mutiny;

import io.smallrye.mutiny.Uni;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import org.HdrHistogram.Histogram;

/**
 * Generates requests on its own event loop at a fixed average rate (open loop),
 * so that the load doesn't depend on how fast the requests complete,
 * and no other thread is involved in starting or completing a request.
 * <p>
 * The time between two requests is random (Poisson arrivals), so requests sometimes arrive in bursts.
 * Each request opens a session and runs {@code queriesPerSession} {@code find}s.
 * The latency is measured from the time the request was scheduled to start,
 * so a request that starts late because the event loop was busy counts as slow.
 * <p>
 * All the fields are only accessed on the event loop of the verticle.
 */
final class LoadVerticle extends AbstractVerticle {

	private static final long MAX_LATENCY_NS = TimeUnit.SECONDS.toNanos( 60 );

	private final Mutiny.SessionFactory sessionFactory;
	private final int queriesPerSession;
	private final SplittableRandom random;
	private final boolean readOnly;
	// A native query that sleeps in the database for each session, or null
	private final String sleepSql;

	private double ratePerSecond;

	private long timerId = -1;
	private long nextArrivalNs;
	private boolean stopped;

	private boolean recording;
	private Histogram histogram = new Histogram( MAX_LATENCY_NS, 3 );
	private long completed;
	private long scheduled;
	private long failed;
	private int inFlight;
	private int maxInFlight;
	private Throwable firstFailure;

	LoadVerticle(Mutiny.SessionFactory sessionFactory, double ratePerSecond, int queriesPerSession, long seed) {
		this( sessionFactory, ratePerSecond, queriesPerSession, seed, false, 0 );
	}

	/**
	 * @param readOnly make the session read-only ({@code setDefaultReadOnly(true)})
	 * @param dbSleepMs when &gt; 0, each session starts with a native query that sleeps this long in the database
	 * ({@link Workload#sleepSql(double)}), to simulate a slower database
	 */
	LoadVerticle(Mutiny.SessionFactory sessionFactory, double ratePerSecond, int queriesPerSession, long seed, boolean readOnly,
			double dbSleepMs) {
		this.sessionFactory = sessionFactory;
		this.ratePerSecond = ratePerSecond;
		this.queriesPerSession = queriesPerSession;
		this.random = new SplittableRandom( seed );
		this.readOnly = readOnly;
		this.sleepSql = dbSleepMs > 0 ? Workload.sleepSql( dbSleepMs ) : null;
	}

	@Override
	public void start() {
		restartArrivals();
	}

	private void restartArrivals() {
		if ( timerId >= 0 ) {
			vertx.cancelTimer( timerId );
			timerId = -1;
		}
		if ( ratePerSecond > 0 && !stopped ) {
			nextArrivalNs = System.nanoTime() + nextIntervalNs();
			scheduleNext();
		}
	}

	// A timer for each request, not a periodic one: a timer that fires with nothing to do would wake up the event loop
	// and count as a context switch. Vert.x timers have a 1 ms resolution, so the requests that are due are started
	// together, at the latest 1 ms late. The latency includes this delay.
	private void scheduleNext() {
		long delayMs = Math.max( 1, TimeUnit.NANOSECONDS.toMillis( nextArrivalNs - System.nanoTime() ) );
		timerId = vertx.setTimer( delayMs, id -> tick() );
	}

	private void tick() {
		if ( stopped ) {
			return;
		}
		long now = System.nanoTime();
		while ( nextArrivalNs <= now ) {
			send( nextArrivalNs );
			nextArrivalNs += nextIntervalNs();
		}
		scheduleNext();
	}

	private long nextIntervalNs() {
		// Exponential distribution: the intervals of a Poisson process
		return (long) ( -Math.log( 1.0 - random.nextDouble() ) * 1_000_000_000.0 / ratePerSecond );
	}

	private void send(long scheduledStartNs) {
		if ( recording ) {
			scheduled++;
		}
		inFlight++;
		maxInFlight = Math.max( maxInFlight, inFlight );
		session().subscribe().with(
				v -> onComplete( scheduledStartNs, null ),
				t -> onComplete( scheduledStartNs, t )
		);
	}

	private Uni<Void> session() {
		// Not withSession(): it reuses the session stored in the Vert.x context, and the requests of this verticle
		// all run on the same context. Quarkus uses a different (duplicated) context for each request instead.
		return sessionFactory.openSession().chain( session -> {
			if ( readOnly ) {
				session.setDefaultReadOnly( true );
			}
			Uni<?> chain = Uni.createFrom().voidItem();
			if ( sleepSql != null ) {
				chain = chain.chain( () -> session.createNativeQuery( sleepSql ).getSingleResult() );
			}
			for ( int i = 0; i < queriesPerSession; i++ ) {
				int id = random.nextInt( Workload.WORLD_COUNT ) + 1;
				chain = chain.chain( () -> session.find( World.class, id ) );
			}
			return chain.replaceWithVoid().eventually( session::close );
		} );
	}

	private void onComplete(long scheduledStartNs, Throwable failure) {
		long endNs = System.nanoTime();
		if ( Vertx.currentContext() != context ) {
			// The fields are confined to the verticle event loop
			context.runOnContext( v -> record( scheduledStartNs, endNs, failure ) );
		}
		else {
			record( scheduledStartNs, endNs, failure );
		}
	}

	private void record(long scheduledStartNs, long endNs, Throwable failure) {
		inFlight--;
		if ( !recording ) {
			return;
		}
		if ( failure != null ) {
			failed++;
			if ( firstFailure == null ) {
				firstFailure = failure;
			}
		}
		else {
			completed++;
			histogram.recordValue( Math.min( endNs - scheduledStartNs, MAX_LATENCY_NS ) );
		}
	}

	/**
	 * Start recording the latency and counting the requests, from now on.
	 */
	Future<Void> startRecording() {
		Promise<Void> promise = Promise.promise();
		context.runOnContext( v -> {
			histogram = new Histogram( MAX_LATENCY_NS, 3 );
			completed = 0;
			scheduled = 0;
			failed = 0;
			maxInFlight = inFlight;
			firstFailure = null;
			recording = true;
			promise.complete();
		} );
		return promise.future();
	}

	/**
	 * Stop recording, and return what was recorded since {@link #startRecording()}.
	 */
	Future<Results> stopRecording() {
		Promise<Results> promise = Promise.promise();
		context.runOnContext( v -> {
			recording = false;
			promise.complete( new Results( histogram, completed, scheduled, failed, maxInFlight, firstFailure ) );
		} );
		return promise.future();
	}

	/**
	 * Change the rate from now on.
	 */
	Future<Void> setRate(double ratePerSecond) {
		Promise<Void> promise = Promise.promise();
		context.runOnContext( v -> {
			this.ratePerSecond = ratePerSecond;
			restartArrivals();
			promise.complete();
		} );
		return promise.future();
	}

	/**
	 * Stop starting new requests.
	 *
	 * @return the number of requests that are still running
	 */
	Future<Integer> stopLoad() {
		Promise<Integer> promise = Promise.promise();
		context.runOnContext( v -> {
			stopped = true;
			if ( timerId >= 0 ) {
				vertx.cancelTimer( timerId );
			}
			promise.complete( inFlight );
		} );
		return promise.future();
	}

	Future<Integer> inFlight() {
		Promise<Integer> promise = Promise.promise();
		context.runOnContext( v -> promise.complete( inFlight ) );
		return promise.future();
	}

	Context context() {
		return context;
	}

	record Results(Histogram histogram, long completed, long scheduled, long failed, int maxInFlight, Throwable firstFailure) {
	}
}

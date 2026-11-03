/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;

import org.HdrHistogram.Histogram;

/**
 * Runs one {@link LoadVerticle} on each event loop of a {@link ReactiveSetup}, at the same rate,
 * and lets another benchmark change the rate and read what was recorded.
 * The rate is the total for all the event loops.
 */
public final class ReactiveLoad {

	/**
	 * What was recorded between {@link #startRecording()} and {@link #stopRecording()}.
	 *
	 * @param latency the latency in ns, from the scheduled start of the requests that completed
	 * @param scheduled the requests that were scheduled to start
	 * @param maxInFlight the most requests running at the same time on one event loop (a thread for the ORM)
	 */
	public record Recording(Histogram latency, long completed, long scheduled, long failed, int maxInFlight, Throwable firstFailure) {
	}

	private final List<LoadVerticle> verticles = new ArrayList<>();

	public ReactiveLoad(ReactiveSetup setup, int eventLoops, double rate, int queriesPerSession, double dbSleepMs, long seed) {
		for ( int i = 0; i < eventLoops; i++ ) {
			var verticle = new LoadVerticle( setup.sessionFactory, rate / eventLoops, queriesPerSession, seed + i, true, dbSleepMs );
			join( setup.vertx.deployVerticle( verticle, new DeploymentOptions() ) );
			verticles.add( verticle );
		}
	}

	public void setRate(double rate) {
		for ( var verticle : verticles ) {
			join( verticle.setRate( rate / verticles.size() ) );
		}
	}

	public void startRecording() {
		for ( var verticle : verticles ) {
			join( verticle.startRecording() );
		}
	}

	public Recording stopRecording() {
		Histogram latency = new Histogram( TimeUnit.SECONDS.toNanos( 60 ), 3 );
		long completed = 0;
		long scheduled = 0;
		long failed = 0;
		int maxInFlight = 0;
		Throwable firstFailure = null;
		for ( var verticle : verticles ) {
			LoadVerticle.Results results = join( verticle.stopRecording() );
			latency.add( results.histogram() );
			completed += results.completed();
			scheduled += results.scheduled();
			failed += results.failed();
			maxInFlight = Math.max( maxInFlight, results.maxInFlight() );
			if ( firstFailure == null ) {
				firstFailure = results.firstFailure();
			}
		}
		return new Recording( latency, completed, scheduled, failed, maxInFlight, firstFailure );
	}

	public int inFlight() {
		int inFlight = 0;
		for ( var verticle : verticles ) {
			inFlight += join( verticle.inFlight() );
		}
		return inFlight;
	}

	/**
	 * Stop starting requests, and wait until the ones running completed.
	 *
	 * @return the number of requests still running when the timeout expired
	 */
	public int stop(long timeoutMs) throws InterruptedException {
		for ( var verticle : verticles ) {
			join( verticle.stopLoad() );
		}
		return drain( timeoutMs );
	}

	/**
	 * Wait until the requests running completed (the rate should be 0).
	 *
	 * @return the number of requests still running when the timeout expired
	 */
	public int drain(long timeoutMs) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos( timeoutMs );
		int inFlight = inFlight();
		while ( inFlight > 0 && System.nanoTime() < deadline ) {
			TimeUnit.MILLISECONDS.sleep( 10 );
			inFlight = inFlight();
		}
		return inFlight;
	}

	private static <T> T join(Future<T> future) {
		return future.toCompletionStage().toCompletableFuture().join();
	}
}

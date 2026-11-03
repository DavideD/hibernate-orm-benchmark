/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.benchmark.comparison;

import org.hibernate.reactive.benchmark.stealing.ReactiveLoad;
import org.hibernate.reactive.benchmark.stealing.ReactiveSetup;

/**
 * Hibernate Reactive: a {@code LoadVerticle} on each event loop starts the requests itself (the same as for the steal benchmark).
 */
final class ReactiveEngine implements LoadEngine {

	private final ReactiveSetup setup;
	private final ReactiveLoad load;

	ReactiveEngine(int eventLoops, String poolType, int poolSize, long localWaitUs, boolean nativeTransport, int pipeliningLimit,
			int queriesPerSession, double dbSleepMs, boolean preparedStatements, long seed) {
		// The steals aren't counted
		setup = new ReactiveSetup( eventLoops, poolType, poolSize, localWaitUs, nativeTransport,
				new ReactiveSetup.ClientOptions( preparedStatements, pipeliningLimit, false ) );
		if ( setup.nativeTransport() != nativeTransport ) {
			setup.close();
			throw new IllegalStateException( "nativeTransport=" + nativeTransport + ", but Vert.x uses "
					+ ( setup.nativeTransport() ? "the native transport" : "NIO (is netty-transport-native-epoll on the classpath? build with EPOLL)" ) );
		}
		load = new ReactiveLoad( setup, eventLoops, 0, queriesPerSession, dbSleepMs, seed );
	}

	@Override
	public void setRate(double ratePerSecond) {
		load.setRate( ratePerSecond );
	}

	@Override
	public void startRecording() {
		load.startRecording();
	}

	@Override
	public ReactiveLoad.Recording stopRecording() {
		return load.stopRecording();
	}

	@Override
	public int drain(long timeoutMs) throws InterruptedException {
		return load.drain( timeoutMs );
	}

	@Override
	public void close() throws InterruptedException {
		load.stop( 30_000 );
		setup.close();
	}
}

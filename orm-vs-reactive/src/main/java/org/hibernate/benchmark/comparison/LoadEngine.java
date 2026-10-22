/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.benchmark.comparison;

import org.hibernate.reactive.benchmark.stealing.ReactiveLoad;

/**
 * Generates requests at a fixed average rate (open loop) on the stack under test, and records what happened.
 * A request is a session that runs the same statements on both stacks, see {@code Workload}.
 */
interface LoadEngine extends AutoCloseable {

	/**
	 * Change the total rate, in requests per second, from now on. 0 stops starting requests.
	 */
	void setRate(double ratePerSecond);

	/**
	 * Start recording the requests that complete, and counting the ones scheduled, from now on.
	 */
	void startRecording();

	ReactiveLoad.Recording stopRecording();

	/**
	 * Wait until the requests running completed (the rate should be 0).
	 *
	 * @return the number of requests still running when the timeout expired
	 */
	int drain(long timeoutMs) throws InterruptedException;

	@Override
	void close() throws Exception;
}

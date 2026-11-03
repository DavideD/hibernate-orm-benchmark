/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

/**
 * The settings of {@code poolType=localWait}, see {@code LocalWaitPool}.
 *
 * @param localWaitUs how long a request waits for a connection of its own event loop before it can take another one,
 * in microseconds
 * @param depth the local wait is skipped when the event loop already has more than {@code depth} &times; its
 * number of connections local waiters (0 to always wait)
 * @param creationAffinity open the new connections on the event loop of the request that needs them
 * @param stats what happened to the waiters
 */
record LocalWaitConfig(long localWaitUs, int depth, boolean creationAffinity, LocalWaitStats stats) {
}

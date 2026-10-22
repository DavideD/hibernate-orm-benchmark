/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.lang.reflect.Field;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.vertx.core.Vertx;
import io.vertx.core.internal.pool.Executor;
import io.vertx.core.internal.pool.Task;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;

/**
 * Makes a Vert.x pool process the pending releases of connections before the other actions, like an acquisition.
 * <p>
 * The Vert.x pool ({@code SimpleConnectionPool}) processes all its actions from a single queue, in the order
 * they're submitted. An acquisition that finds no free connection on its event loop takes one from another
 * event loop, or waits, even if the release of a connection of its event loop is right behind it in the queue.
 * With this executor, releases and the other actions go to separate queues. Each time the pool runs,
 * it first processes all the pending releases, then the next other action: an acquisition sees every connection
 * whose release has been submitted.
 * <p>
 * It wraps the executor of the pool: the actions still run one at a time, in the same {@code CombinerExecutor}.
 * It depends on the internals of Vert.x 5. The copy in {@code src/vertx4/java} does the same for Vert.x 4.
 */
final class ReleasesFirstExecutor implements Executor<Object> {

	private final Executor<Object> combiner;
	private final Queue<Action<Object>> releases = new ConcurrentLinkedQueue<>();
	private final Queue<Action<Object>> others = new ConcurrentLinkedQueue<>();
	private final Action<Object> next = this::next;

	private ReleasesFirstExecutor(Executor<Object> combiner) {
		this.combiner = combiner;
	}

	/**
	 * Create a Vert.x pool that processes the releases first.
	 */
	@SuppressWarnings("unchecked")
	static Pool createPool(Vertx vertx, SqlConnectOptions connectOptions, PoolOptions poolOptions) {
		Pool pool = Pool.pool( vertx, connectOptions, poolOptions );
		try {
			Object connectionPool = PoolReflection.connectionPool( pool );
			Field executorField = PoolReflection.executorField( connectionPool );
			var combiner = (Executor<Object>) executorField.get( connectionPool );
			executorField.set( connectionPool, new ReleasesFirstExecutor( combiner ) );
		}
		catch (ReflectiveOperationException e) {
			throw new IllegalStateException( "Can't instrument the pool, the Vert.x internals have changed", e );
		}
		return pool;
	}

	@Override
	public void submit(Action<Object> action) {
		if ( isRelease( action ) ) {
			releases.add( action );
		}
		else {
			others.add( action );
		}
		// One call to next() for each action: every action runs, even if an earlier call already ran it
		combiner.submit( next );
	}

	private static boolean isRelease(Action<Object> action) {
		// AffinityProbe.Release when the probe is installed on top of this executor
		return action instanceof AffinityProbe.Release || "Recycle".equals( action.getClass().getSimpleName() );
	}

	// Run by the combiner, one at a time
	private Task next(Object pool) {
		Task head = null;
		Task tail = null;
		Action<Object> action;
		while ( ( action = releases.poll() ) != null ) {
			Task task = action.execute( pool );
			if ( task != null ) {
				if ( head == null ) {
					head = task;
				}
				else {
					tail.next( task );
				}
				tail = task.last();
			}
		}
		if ( ( action = others.poll() ) != null ) {
			Task task = action.execute( pool );
			if ( task != null ) {
				if ( head == null ) {
					head = task;
				}
				else {
					tail.next( task );
				}
			}
		}
		return head;
	}
}

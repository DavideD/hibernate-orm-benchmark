/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.util.Locale;

/**
 * What the benchmarks that compare Hibernate ORM with Hibernate Reactive have in common.
 */
public final class Workload {

	/**
	 * The {@link World} ids are 1 to this value.
	 */
	public static final int WORLD_COUNT = ReactiveSetup.WORLD_COUNT;

	private Workload() {
	}

	/**
	 * A native query, with a constant text so that the statement can be cached, that returns one row after the
	 * database slept for {@code ms}. Both stacks run it as the first statement of a session to simulate a slower database.
	 * (The row is there because the Vert.x client can't decode the {@code void} that {@code pg_sleep} returns.)
	 */
	public static String sleepSql(double ms) {
		return String.format( Locale.ROOT, "select count(*) from (select pg_sleep(%.6f)) s", ms / 1000.0 );
	}
}

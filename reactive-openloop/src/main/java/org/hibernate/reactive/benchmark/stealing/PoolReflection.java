/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.lang.reflect.Field;

import io.vertx.sqlclient.Pool;

/**
 * Reflection on the private fields of the Vert.x pool, used to instrument it
 * ({@link AffinityProbe}, {@link ReleasesFirstExecutor}). The field names are the same in Vert.x 4.5 and 5.
 */
final class PoolReflection {

	private PoolReflection() {
	}

	/**
	 * @param pool a pool created with {@link Pool#pool}, not a proxy
	 * @return the {@code SimpleConnectionPool} that manages the connections of the pool
	 */
	static Object connectionPool(Pool pool) throws ReflectiveOperationException {
		// The pool can be a wrapper (PgPoolImpl in Vert.x 4) of the pool implementation (PoolImpl)
		Object current = pool;
		Field delegate;
		while ( ( delegate = fieldOrNull( current.getClass(), "delegate" ) ) != null ) {
			current = delegate.get( current );
		}
		Object sqlConnectionPool = field( current.getClass(), "pool" ).get( current );
		return field( sqlConnectionPool.getClass(), "pool" ).get( sqlConnectionPool );
	}

	/**
	 * @return the field of the executor that runs all the actions of the {@code SimpleConnectionPool}
	 */
	static Field executorField(Object connectionPool) throws NoSuchFieldException {
		return field( connectionPool.getClass(), "sync" );
	}

	static Object read(Object target, Field field) {
		try {
			return field.get( target );
		}
		catch (IllegalAccessException e) {
			throw new IllegalStateException( e );
		}
	}

	static Field field(Class<?> type, String name) throws NoSuchFieldException {
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

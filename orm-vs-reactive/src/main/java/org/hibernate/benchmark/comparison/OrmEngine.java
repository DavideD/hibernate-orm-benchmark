/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.benchmark.comparison;

import java.time.Duration;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.benchmark.stealing.ReactiveLoad;
import org.hibernate.reactive.benchmark.stealing.Workload;
import org.hibernate.reactive.benchmark.stealing.World;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Hibernate ORM with blocking JDBC (pgjdbc) and an Agroal pool. A scheduler thread submits the requests to an executor
 * at the scheduled times, the same random times as {@code LoadVerticle} (Poisson arrivals, at most every 1 ms),
 * and a thread of the executor runs each request: a session with the same statements as the reactive one.
 * The executor is a fixed pool of platform threads, or a virtual thread for each request.
 */
final class OrmEngine implements LoadEngine {

	private static final long MAX_LATENCY_NS = TimeUnit.SECONDS.toNanos( 60 );

	private final SessionFactory sessionFactory;
	private final ExecutorService executor;
	private final Thread scheduler;
	private final int queriesPerSession;
	private final String sleepSql;

	private volatile double ratePerSecond;
	// Incremented when the rate changes, so that the scheduler starts a new sequence of arrivals
	private volatile int rateVersion;
	private volatile boolean stopped;
	private volatile boolean recording;

	private final AtomicInteger inFlight = new AtomicInteger();
	private final AtomicInteger maxInFlight = new AtomicInteger();
	private final LongAdder completed = new LongAdder();
	private final LongAdder scheduled = new LongAdder();
	private final LongAdder failed = new LongAdder();
	private volatile Throwable firstFailure;
	private final Recorder latency = new Recorder( MAX_LATENCY_NS, 3 );

	/**
	 * @param executorType {@code platform} for a fixed pool of {@code threads} platform threads,
	 * or {@code virtual} for a virtual thread for each request
	 */
	OrmEngine(int poolSize, String executorType, int threads, int queriesPerSession, double dbSleepMs, boolean preparedStatements, long seed) {
		this.queriesPerSession = queriesPerSession;
		this.sleepSql = dbSleepMs > 0 ? Workload.sleepSql( dbSleepMs ) : null;

		var config = new Configuration();
		config.addAnnotatedClass( World.class );
		// pgjdbc caches the prepared statements (preparedStatementCacheQueries=256 is the default) and switches to
		// server-side prepared statements after 5 executions (prepareThreshold=5, the default)
		config.setProperty( AvailableSettings.JAKARTA_JDBC_URL, "jdbc:postgresql://localhost:5432/hibernate_orm_test"
				+ ( preparedStatements ? "" : "?preparedStatementCacheQueries=0&prepareThreshold=0" ) );
		config.setProperty( AvailableSettings.JAKARTA_JDBC_USER, "hibernate_orm_test" );
		config.setProperty( AvailableSettings.JAKARTA_JDBC_PASSWORD, "hibernate_orm_test" );
		config.setProperty( AvailableSettings.CONNECTION_PROVIDER, "org.hibernate.agroal.internal.AgroalConnectionProvider" );
		config.setProperty( "hibernate.agroal.minSize", String.valueOf( poolSize ) );
		config.setProperty( "hibernate.agroal.initialSize", String.valueOf( poolSize ) );
		config.setProperty( "hibernate.agroal.maxSize", String.valueOf( poolSize ) );
		config.setProperty( "hibernate.agroal.acquisitionTimeout", "PT30S" );
		// The Vert.x client doesn't start a transaction for a session without one: its statements are in auto-commit mode
		config.setProperty( AvailableSettings.AUTOCOMMIT, "true" );
		config.setProperty( AvailableSettings.SHOW_SQL, "false" );
		config.setProperty( AvailableSettings.FORMAT_SQL, "false" );
		config.setProperty( AvailableSettings.HBM2DDL_AUTO, "create" );
		config.setProperty( AvailableSettings.STATEMENT_BATCH_SIZE, "0" );
		config.setProperty( "hibernate.generate_statistics", "false" );
		sessionFactory = config.buildSessionFactory();

		populateWorlds();

		executor = switch ( executorType ) {
			case "platform" -> {
				var pool = new ThreadPoolExecutor( threads, threads, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
						namedThreads() );
				pool.prestartAllCoreThreads();
				yield pool;
			}
			case "virtual" -> Executors.newThreadPerTaskExecutor( Thread.ofVirtual().name( "request-", 0 ).factory() );
			default -> throw new IllegalArgumentException( "Unknown executor: " + executorType + " (valid: platform, virtual)" );
		};
		scheduler = new Thread( () -> schedule( seed ), "load-scheduler" );
		scheduler.setDaemon( true );
		scheduler.start();
	}

	private static ThreadFactory namedThreads() {
		var count = new AtomicInteger();
		return runnable -> new Thread( runnable, "request-" + count.getAndIncrement() );
	}

	private void populateWorlds() {
		try (Session session = sessionFactory.openSession()) {
			session.getTransaction().begin();
			var random = new SplittableRandom();
			for ( int i = 1; i <= Workload.WORLD_COUNT; i++ ) {
				World world = new World();
				world.setId( i );
				world.setRandomNumber( random.nextInt( 10_000 ) );
				session.persist( world );
				if ( i % 500 == 0 ) {
					session.flush();
					session.clear();
				}
			}
			session.getTransaction().commit();
		}
	}

	private void schedule(long seed) {
		var random = new SplittableRandom( seed );
		int version = -1;
		double rate = 0;
		long nextArrivalNs = 0;
		while ( !stopped ) {
			if ( version != rateVersion ) {
				version = rateVersion;
				rate = ratePerSecond;
				nextArrivalNs = rate > 0 ? System.nanoTime() + nextIntervalNs( random, rate ) : 0;
			}
			if ( rate <= 0 ) {
				LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 100 ) );
				continue;
			}
			long now = System.nanoTime();
			while ( nextArrivalNs <= now ) {
				submit( nextArrivalNs, random.split() );
				nextArrivalNs += nextIntervalNs( random, rate );
			}
			// Like the timers of LoadVerticle: at least 1 ms, so the requests that are due are started together, at most 1 ms late
			long delayMs = Math.max( 1, TimeUnit.NANOSECONDS.toMillis( nextArrivalNs - System.nanoTime() ) );
			LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( delayMs ) );
		}
	}

	private static long nextIntervalNs(SplittableRandom random, double rate) {
		// Exponential distribution: the intervals of a Poisson process
		return (long) ( -Math.log( 1.0 - random.nextDouble() ) * 1_000_000_000.0 / rate );
	}

	private void submit(long scheduledStartNs, SplittableRandom random) {
		if ( recording ) {
			scheduled.increment();
		}
		int running = inFlight.incrementAndGet();
		maxInFlight.accumulateAndGet( running, Math::max );
		executor.execute( () -> run( scheduledStartNs, random ) );
	}

	private void run(long scheduledStartNs, SplittableRandom random) {
		Throwable failure = null;
		try (Session session = sessionFactory.openSession()) {
			session.setDefaultReadOnly( true );
			if ( sleepSql != null ) {
				session.createNativeQuery( sleepSql ).getSingleResult();
			}
			for ( int i = 0; i < queriesPerSession; i++ ) {
				session.find( World.class, random.nextInt( Workload.WORLD_COUNT ) + 1 );
			}
		}
		catch (Throwable t) {
			failure = t;
		}
		long endNs = System.nanoTime();
		inFlight.decrementAndGet();
		if ( recording ) {
			if ( failure != null ) {
				failed.increment();
				if ( firstFailure == null ) {
					firstFailure = failure;
				}
			}
			else {
				completed.increment();
				latency.recordValue( Math.min( endNs - scheduledStartNs, MAX_LATENCY_NS ) );
			}
		}
	}

	@Override
	public void setRate(double ratePerSecond) {
		this.ratePerSecond = ratePerSecond;
		rateVersion++;
		LockSupport.unpark( scheduler );
	}

	@Override
	public void startRecording() {
		latency.reset();
		completed.reset();
		scheduled.reset();
		failed.reset();
		firstFailure = null;
		maxInFlight.set( inFlight.get() );
		recording = true;
	}

	@Override
	public ReactiveLoad.Recording stopRecording() {
		recording = false;
		Histogram histogram = latency.getIntervalHistogram();
		return new ReactiveLoad.Recording( histogram, completed.sum(), scheduled.sum(), failed.sum(), maxInFlight.get(), firstFailure );
	}

	@Override
	public int drain(long timeoutMs) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos( timeoutMs );
		while ( inFlight.get() > 0 && System.nanoTime() < deadline ) {
			TimeUnit.MILLISECONDS.sleep( 10 );
		}
		return inFlight.get();
	}

	@Override
	public void close() throws InterruptedException {
		setRate( 0 );
		drain( 30_000 );
		stopped = true;
		LockSupport.unpark( scheduler );
		scheduler.join();
		executor.shutdown();
		executor.awaitTermination( 30, TimeUnit.SECONDS );
		sessionFactory.close();
	}
}

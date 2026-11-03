/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;

/**
 * Netty only: an event loop with one scheduled task that runs again every {@code periodMs} milliseconds, like a Vert.x
 * timer, and a few thousand tiny tasks per second submitted from another thread at random times, like I/O events.
 * Prints the CPU time of the event loop thread: with Netty 4.1 it's what the work costs, with 4.2 it's much more
 * because the event loop polls the selector in a loop (selectNow) in the last 0.5 ms before the timer.
 * Same code for Netty 4.1 and 4.2: run it with the jars of each version on the class path:
 * <pre>java -cp 'netty-4.2/*' NettyTimerSpin.java 1 5</pre>
 * Arguments: periodMs (default 1), seconds (default 5), submitted tasks per second (default 4000, 0 = none).
 */
public class NettyTimerSpin {

	public static void main(String[] args) throws Exception {
		long periodMs = args.length > 0 ? Long.parseLong( args[0] ) : 1;
		long seconds = args.length > 1 ? Long.parseLong( args[1] ) : 5;
		double perSecond = args.length > 2 ? Double.parseDouble( args[2] ) : 4000;
		var group = new NioEventLoopGroup( 1 );
		EventLoop loop = group.next();
		var runs = new AtomicLong();
		var threadId = new AtomicLong();
		Runnable[] task = new Runnable[1];
		task[0] = () -> {
			threadId.set( Thread.currentThread().threadId() );
			runs.incrementAndGet();
			loop.schedule( task[0], periodMs, TimeUnit.MILLISECONDS );
		};
		loop.schedule( task[0], periodMs, TimeUnit.MILLISECONDS );
		if ( perSecond > 0 ) {
			var random = new java.util.SplittableRandom( 42 );
			var submitter = new Thread( () -> {
				long next = System.nanoTime();
				while ( true ) {
					// Exponential distribution: the intervals of a Poisson process
					next += (long) ( -Math.log( 1.0 - random.nextDouble() ) * 1e9 / perSecond );
					long wait;
					while ( ( wait = next - System.nanoTime() ) > 0 ) {
						java.util.concurrent.locks.LockSupport.parkNanos( wait );
					}
					try {
						loop.execute( () -> { } );
					}
					catch (java.util.concurrent.RejectedExecutionException e) {
						return;
					}
				}
			}, "submitter" );
			submitter.setDaemon( true );
			submitter.start();
		}
		TimeUnit.SECONDS.sleep( 1 );
		var threads = ManagementFactory.getThreadMXBean();
		long cpuBefore = threads.getThreadCpuTime( threadId.get() );
		long runsBefore = runs.get();
		TimeUnit.SECONDS.sleep( seconds );
		long cpuMs = TimeUnit.NANOSECONDS.toMillis( threads.getThreadCpuTime( threadId.get() ) - cpuBefore );
		System.out.printf( "Netty %s, task every %d ms: %d runs, event loop CPU %d ms in %d s (%.1f%% of a core)%n",
				io.netty.util.Version.identify().get( "netty-transport" ).artifactVersion(), periodMs, runs.get() - runsBefore,
				cpuMs, seconds, 100.0 * cpuMs / ( seconds * 1000 ) );
		group.shutdownGracefully( 0, 0, TimeUnit.SECONDS );
	}
}

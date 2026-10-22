/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.benchmark.comparison;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

/**
 * The time the CPUs that the JVM may run on (its affinity, e.g. set by {@code taskset}) were busy, from {@code /proc/stat}.
 * Unlike the CPU time of the process, it includes everything that ran on these CPUs: the kernel's network stack
 * (soft interrupts), which loopback traffic to the database makes significant, and other processes.
 * It shows whether the CPUs are the bottleneck. Linux only.
 *
 * @param busyTicks all the ticks but idle and iowait
 * @param softirqTicks the ticks of soft interrupts, included in the busy ticks
 */
record CpuUsage(long busyTicks, long softirqTicks) {

	private static final Set<Integer> CPUS = allowedCpus();

	static CpuUsage take() {
		long busy = 0;
		long softirq = 0;
		try {
			for ( String line : Files.readAllLines( Path.of( "/proc/stat" ) ) ) {
				if ( !line.startsWith( "cpu" ) || line.startsWith( "cpu " ) ) {
					continue;
				}
				String[] fields = line.split( "\\s+" );
				if ( !CPUS.contains( Integer.parseInt( fields[0].substring( 3 ) ) ) ) {
					continue;
				}
				// user nice system idle iowait irq softirq steal
				long user = Long.parseLong( fields[1] );
				long nice = Long.parseLong( fields[2] );
				long system = Long.parseLong( fields[3] );
				long irq = Long.parseLong( fields[6] );
				long soft = Long.parseLong( fields[7] );
				long steal = Long.parseLong( fields[8] );
				busy += user + nice + system + irq + soft + steal;
				softirq += soft;
			}
		}
		catch (IOException e) {
			throw new IllegalStateException( "Can't read /proc/stat", e );
		}
		return new CpuUsage( busy, softirq );
	}

	/**
	 * @return the number of CPUs that the JVM may run on
	 */
	static int cpuCount() {
		return CPUS.size();
	}

	// The ticks are in USER_HZ (1/100 s)
	double busyCores(CpuUsage before, double seconds) {
		return ( busyTicks - before.busyTicks ) / 100.0 / seconds;
	}

	double softirqCores(CpuUsage before, double seconds) {
		return ( softirqTicks - before.softirqTicks ) / 100.0 / seconds;
	}

	private static Set<Integer> allowedCpus() {
		try {
			for ( String line : Files.readAllLines( Path.of( "/proc/self/status" ) ) ) {
				if ( line.startsWith( "Cpus_allowed_list:" ) ) {
					Set<Integer> cpus = new HashSet<>();
					for ( String range : line.substring( line.indexOf( ':' ) + 1 ).trim().split( "," ) ) {
						String[] bounds = range.split( "-" );
						int first = Integer.parseInt( bounds[0] );
						int last = bounds.length > 1 ? Integer.parseInt( bounds[1] ) : first;
						for ( int cpu = first; cpu <= last; cpu++ ) {
							cpus.add( cpu );
						}
					}
					return cpus;
				}
			}
		}
		catch (IOException e) {
			throw new IllegalStateException( "Can't read /proc/self/status", e );
		}
		throw new IllegalStateException( "No Cpus_allowed_list in /proc/self/status" );
	}
}

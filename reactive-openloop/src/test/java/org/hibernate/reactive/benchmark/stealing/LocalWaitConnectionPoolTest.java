/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.benchmark.stealing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.hibernate.reactive.benchmark.stealing.LocalWaitPoolHarness.Request;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The order in which {@code LocalWaitConnectionPool} gives the connections to the waiters. No database,
 * and no real time: the timers of the local waits only fire when the test fires them.
 * <p>
 * Most tests use 2 connections and 2 event loops: {@code a} holds the connection of the event loop 0,
 * {@code b} the one of the event loop 1.
 */
class LocalWaitConnectionPoolTest {

	private static LocalWaitPoolHarness twoLoops() {
		return new LocalWaitPoolHarness( 2, 2, 500, 0, true, false );
	}

	@Test
	void aReleasedConnectionGoesToTheOldestLocalWaiterOfItsEventLoopFirst() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			Request b = h.acquire( 1 );
			assertEquals( 0, a.connection().loop() );
			assertEquals( 1, b.connection().loop() );

			Request waitsOn0 = h.acquire( 0 );
			Request firstOn1 = h.acquire( 1 );
			Request secondOn1 = h.acquire( 1 );
			assertFalse( waitsOn0.isServed() || firstOn1.isServed() || secondOn1.isServed() );

			// The waiter of the event loop 0 is older, but the connection is not on its event loop
			b.release();
			assertEquals( 1, firstOn1.connection().loop() );
			assertFalse( waitsOn0.isServed() );
			assertFalse( secondOn1.isServed() );

			a.release();
			assertEquals( 0, waitsOn0.connection().loop() );
			assertFalse( secondOn1.isServed() );

			firstOn1.release();
			assertEquals( 1, secondOn1.connection().loop() );
			assertTrue( h.timers.stream().allMatch( t -> t.cancelled ), "The timers of the waiters served are cancelled" );
			assertEquals( 3, h.stats.servedLocal.sum() );
		}
	}

	@Test
	void aLocalWaiterDoesNotTakeAFreeConnectionOfAnotherEventLoop() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			Request b = h.acquire( 1 );
			b.release();

			// The connection of the event loop 1 is free, but this request only waits for one of the event loop 0
			Request waiter = h.acquire( 0 );
			assertFalse( waiter.isServed() );
			assertEquals( 1, h.stats.localWaits.sum() );

			a.release();
			assertEquals( 0, waiter.connection().loop() );
			assertEquals( 1, waiter.served() );
		}
	}

	@Test
	void aConnectionStaysFreeForTheLocalWaitersOfOtherEventLoops() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			Request b = h.acquire( 1 );
			Request waiter = h.acquire( 0 );

			b.release();
			assertFalse( waiter.isServed() );

			// So the connection is still there for the event loop 1
			Request other = h.acquire( 1 );
			assertEquals( 1, other.connection().loop() );
			assertFalse( waiter.isServed() );
			a.release();
			assertTrue( waiter.isServed() );
		}
	}

	@Test
	void aWaiterWhoseWaitIsOverTakesAFreeConnectionOfAnotherEventLoop() {
		try (var h = twoLoops()) {
			h.acquire( 0 );
			Request b = h.acquire( 1 );
			b.release();
			Request waiter = h.acquire( 0 );
			assertFalse( waiter.isServed() );

			h.fire( waiter.timer() );

			assertEquals( 1, waiter.connection().loop() );
			assertEquals( 1, h.stats.expiredStolen.sum() );
		}
	}

	@Test
	void aWaiterWhoseWaitIsOverWaitsForTheFirstConnectionReleasedByAnyEventLoop() {
		try (var h = twoLoops()) {
			h.acquire( 0 );
			Request b = h.acquire( 1 );
			Request waiter = h.acquire( 0 );

			h.fire( waiter.timer() );
			assertFalse( waiter.isServed() );
			assertEquals( 1, h.stats.expiredQueued.sum() );

			b.release();
			assertEquals( 1, waiter.connection().loop() );
			assertEquals( 1, h.stats.servedAfterExpiry.sum() );
		}
	}

	@Test
	void aReleasedConnectionGoesToTheLocalWaiterBeforeTheWaitersWhoseWaitIsOver() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			Request b = h.acquire( 1 );
			Request on0 = h.acquire( 0 );
			Request on1 = h.acquire( 1 );
			// on0 is the oldest, and its wait is over
			h.fire( on0.timer() );

			b.release();
			assertTrue( on1.isServed() );
			assertFalse( on0.isServed() );

			// No local waiter for the event loop 1 any more: on0 gets it
			on1.release();
			assertEquals( 1, on0.connection().loop() );
			a.release();
			assertEquals( 1, on0.served() );
		}
	}

	@Test
	void theWaitersWhoseWaitIsOverAreServedInOrder() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			h.acquire( 1 );
			Request first = h.acquire( 0 );
			Request second = h.acquire( 0 );
			Request third = h.acquire( 0 );
			h.fire( second.timer() );
			h.fire( first.timer() );
			h.fire( third.timer() );

			// The order of the end of the wait, not of the arrival: nothing says which timer fires first
			a.release();
			assertTrue( second.isServed() );
			assertFalse( first.isServed() );
			second.release();
			assertTrue( first.isServed() );
			assertFalse( third.isServed() );
		}
	}

	@Test
	void aWaiterIsNotServedTwice() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			Request b = h.acquire( 1 );
			Request waiter = h.acquire( 0 );
			a.release();
			assertEquals( 1, waiter.served() );
			assertTrue( waiter.timer().cancelled );

			// The timer was already running when the pool cancelled it
			b.release();
			h.fire( waiter.timer() );

			assertEquals( 1, waiter.served() );
			assertEquals( 0, h.stats.expiredStolen.sum() );
			assertEquals( 0, h.stats.expiredQueued.sum() );
			// The connection of the event loop 1 is still free
			assertEquals( 1, h.acquire( 1 ).connection().loop() );
		}
	}

	@Test
	void aCancelledWaiterIsNotServed() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			h.acquire( 1 );
			Request cancelled = h.acquire( 0 );
			Request waiter = h.acquire( 0 );

			cancelled.cancel();
			assertTrue( cancelled.timer().cancelled );
			a.release();

			assertFalse( cancelled.isServed() );
			assertTrue( waiter.isServed() );
			assertEquals( 0, h.pool.waiters() );
		}
	}

	@Test
	void whenTheEventLoopHasManyWaitersTheNextRequestStealsAtOnce() {
		try (var h = new LocalWaitPoolHarness( 2, 2, 500, 1, true, false )) {
			Request a = h.acquire( 0 );
			Request b = h.acquire( 1 );
			// 1 connection on the event loop 0, so up to 1 local waiter
			Request first = h.acquire( 0 );
			Request second = h.acquire( 0 );
			assertEquals( 2, h.stats.localWaits.sum() );
			b.release();

			Request third = h.acquire( 0 );
			assertEquals( 1, h.stats.depthShortcuts.sum() );
			assertEquals( 1, third.connection().loop(), "It takes the free connection of the event loop 1" );
			assertFalse( first.isServed() || second.isServed() );
			assertNull( third.timer() );

			a.release();
			assertEquals( 0, first.connection().loop() );
		}
	}

	@Test
	void whenTheEventLoopHasManyWaitersTheNextRequestQueuesWithoutWaitingForALocalConnection() {
		try (var h = new LocalWaitPoolHarness( 2, 2, 500, 1, true, false )) {
			h.acquire( 0 );
			Request b = h.acquire( 1 );
			Request first = h.acquire( 0 );
			Request second = h.acquire( 0 );
			Request third = h.acquire( 0 );
			assertNull( third.timer(), "No local wait, so no timer" );
			assertEquals( 1, h.stats.depthShortcuts.sum() );

			// The first connection released goes to the oldest waiter whose wait is over: the one that skipped it
			b.release();
			assertTrue( third.isServed() );
			assertFalse( first.isServed() || second.isServed() );
		}
	}

	@Test
	void withoutADepthTheEventLoopAlwaysWaits() {
		try (var h = twoLoops()) {
			h.acquire( 0 );
			h.acquire( 1 );
			List<Request> waiters = new ArrayList<>();
			for ( int i = 0; i < 20; i++ ) {
				waiters.add( h.acquire( 0 ) );
			}
			assertEquals( 20, h.stats.localWaits.sum() );
			assertEquals( 0, h.stats.depthShortcuts.sum() );
			assertTrue( waiters.stream().noneMatch( Request::isServed ) );
		}
	}

	@Test
	void anEventLoopWithoutConnectionsDoesNotWaitForOne() {
		try (var h = new LocalWaitPoolHarness( 1, 2, 500, 0, true, false )) {
			Request a = h.acquire( 0 );
			Request waiter = h.acquire( 1 );
			assertNull( waiter.timer() );
			assertEquals( 1, h.stats.noLocalConnection.sum() );
			assertFalse( waiter.isServed() );

			a.release();
			assertEquals( 0, waiter.connection().loop() );

			// And when a connection is free, it takes it at once
			waiter.release();
			assertEquals( 0, h.acquire( 1 ).connection().loop() );
		}
	}

	@Test
	void theConnectionsAreOpenedOnTheEventLoopThatNeedsThemWhileItHasLessThanItsShare() {
		try (var h = new LocalWaitPoolHarness( 4, 2, 500, 0, true, true )) {
			List<Integer> loops = new ArrayList<>();
			for ( int i = 0; i < 4; i++ ) {
				loops.add( h.acquire( 0 ).connection().loop() );
			}
			// 2 connections for each event loop: the share
			assertEquals( List.of( 0, 0, 1, 1 ), loops );
		}
	}

	@Test
	void withoutTheCreationAffinityTheConnectionsGoToTheEventLoopsInTurn() {
		try (var h = new LocalWaitPoolHarness( 4, 2, 500, 0, false, true )) {
			List<Integer> loops = new ArrayList<>();
			for ( int i = 0; i < 4; i++ ) {
				loops.add( h.acquire( 0 ).connection().loop() );
			}
			assertEquals( List.of( 0, 1, 0, 1 ), loops );
		}
	}

	@Test
	void theEvictedConnectionsAreTheOnesNotInUseAndTheirPlaceIsFree() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			Request b = h.acquire( 1 );
			var aConnection = a.connection();
			a.release();

			// Idle timeout and maximum lifetime: only the connection that isn't in use goes
			assertEquals( List.of( aConnection ), h.evict() );
			assertEquals( 1, h.pool.size() );

			// The pool opens another one
			Request c = h.acquire( 0 );
			assertEquals( 0, c.connection().loop() );
			assertFalse( c.connection().equals( aConnection ) );
			assertEquals( 2, h.pool.size() );
			b.release();
		}
	}

	@Test
	void aClosedConnectionIsReplacedForTheOldestWaiter() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			h.acquire( 1 );
			Request waiter = h.acquire( 1 );
			Request other = h.acquire( 0 );

			// The oldest waiter, whatever its event loop, gets the new connection. It opens on the event loop that lost
			// its connection, the only one under its share, and not on the event loop of the waiter
			h.close( a.connection() );

			assertEquals( 0, waiter.connection().loop() );
			assertFalse( a.connection().equals( waiter.connection() ) );
			assertTrue( waiter.timer().cancelled );
			assertFalse( other.isServed() );
			assertEquals( 2, h.pool.size() );

			// The connection of the lease that was closed isn't used again
			a.release();
			assertFalse( other.isServed() );
			waiter.release();
			assertEquals( 1, other.served() );
		}
	}

	@Test
	void aConnectionThatFailsToOpenFailsItsRequestAndTheNextWaiterOpensAnother() {
		try (var h = twoLoops()) {
			Request a = h.acquire( 0 );
			h.acquire( 1 );
			Request first = h.acquire( 0 );
			Request second = h.acquire( 1 );

			h.failNextConnect();
			h.close( a.connection() );

			assertEquals( "Can't connect", first.failure().getMessage() );
			assertFalse( first.isServed() );
			assertEquals( 1, second.served() );
			assertEquals( 0, h.pool.waiters() );
			assertTrue( h.timers.stream().allMatch( t -> t.cancelled ) );
			assertEquals( 2, h.pool.size() );
		}
	}

	@Test
	void aRequestWhenThePoolIsNotFullThatFailsToOpenAConnectionFreesItsPlace() {
		try (var h = twoLoops()) {
			h.failNextConnect();
			Request failed = h.acquire( 0 );
			assertEquals( "Can't connect", failed.failure().getMessage() );
			assertEquals( 0, h.pool.size() );
			assertEquals( 0, h.pool.requests() );
			assertEquals( 0, h.acquire( 0 ).connection().loop() );
		}
	}

	/**
	 * Random requests, releases and deadlines. Whatever the order is, a connection is never held by 2 requests,
	 * and once everything is released and all the deadlines are over, every request has been served exactly once.
	 */
	@Test
	void noWaiterIsStrandedOrServedTwice() {
		for ( int seed = 0; seed < 25; seed++ ) {
			int depth = seed % 3;
			try (var h = new LocalWaitPoolHarness( 3 + seed % 3, 3, 500, depth, seed % 2 == 0, true )) {
				randomRun( h, new Random( seed ) );
			}
		}
	}

	private static void randomRun(LocalWaitPoolHarness h, Random random) {
		List<Request> requests = new ArrayList<>();
		Set<Request> released = Collections.newSetFromMap( new IdentityHashMap<>() );
		for ( int step = 0; step < 300; step++ ) {
			switch ( random.nextInt( 3 ) ) {
				case 0 -> {
					if ( requests.stream().filter( r -> !r.isServed() ).count() < 15 ) {
						requests.add( h.acquire( random.nextInt( h.loops.size() ) ) );
					}
				}
				case 1 -> releaseOne( requests, released, random );
				default -> {
					var pending = h.timers.stream().filter( t -> !t.cancelled && !t.fired ).toList();
					if ( !pending.isEmpty() ) {
						h.fire( pending.get( random.nextInt( pending.size() ) ) );
					}
				}
			}
			checkNoConnectionHeldTwice( requests, released );
		}
		// Finish: all the deadlines are over, and the connections are released one by one
		while ( requests.stream().anyMatch( r -> !r.isServed() ) ) {
			h.fireDeadlines();
			if ( !releaseOne( requests, released, random ) ) {
				fail( "Stranded waiters: " + requests.stream().filter( r -> !r.isServed() ).count() );
			}
			checkNoConnectionHeldTwice( requests, released );
		}
		while ( releaseOne( requests, released, random ) ) {
			// Release the connections of the last requests
		}
		assertTrue( requests.stream().allMatch( r -> r.served() == 1 ), "Every request is served exactly once" );
		assertEquals( 0, h.pool.waiters() );
	}

	/**
	 * @return false if no request holds a connection
	 */
	private static boolean releaseOne(List<Request> requests, Set<Request> released, Random random) {
		List<Request> held = requests.stream().filter( r -> r.isServed() && !released.contains( r ) ).toList();
		if ( held.isEmpty() ) {
			return false;
		}
		Request request = held.get( random.nextInt( held.size() ) );
		released.add( request );
		request.release();
		return true;
	}

	private static void checkNoConnectionHeldTwice(List<Request> requests, Set<Request> released) {
		Set<Integer> connections = new HashSet<>();
		for ( Request request : requests ) {
			if ( request.isServed() && !released.contains( request ) ) {
				assertTrue( connections.add( request.connection().id() ), "A connection is held by 2 requests" );
			}
		}
	}
}

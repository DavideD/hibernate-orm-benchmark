/*
 * Copyright (c) 2011-2021 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.core.net.impl.pool;

import io.netty.channel.EventLoop;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.ConnectionPoolTooBusyException;
import io.vertx.core.impl.ContextInternal;
import org.hibernate.reactive.benchmark.stealing.LocalWaitStats;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A fork of Vert.x 4.5 {@code SimpleConnectionPool} (same package, to access the package-private members of
 * {@link PoolWaiter}), where a request that finds no free connection on its event loop waits a little for one,
 * instead of taking a free connection of another event loop (a "steal") or queueing with the other waiters.
 * <p>
 * What's different from {@code SimpleConnectionPool} (the rest is the same code):
 * <ul>
 *   <li><b>Acquire</b> ({@code Acquire#execute}): with no free connection on the event loop of the request,
 *   and the pool full, the request becomes a <i>local waiter</i> of its event loop, with a deadline
 *   ({@link Options#localWaitNanos()}) and doesn't take a free connection of another event loop.
 *   A request skips the local wait, and behaves like {@code SimpleConnectionPool}, if its event loop
 *   has no connection, or has more than {@link Options#depth()} &times; its connections local waiters.</li>
 *   <li><b>Recycle</b> ({@code Recycle#execute}, {@link #pollFor}): the released connection goes to the oldest
 *   local waiter of its own event loop, else to the oldest waiter whose local wait is over, else it stays free.
 *   It never goes to a local waiter of another event loop.</li>
 *   <li><b>Deadline</b> ({@code Expire#execute}): when the local wait of a request ends, it takes a free connection
 *   of any event loop, or else joins the queue of the waiters whose local wait is over, served in order by the
 *   first connection released by any event loop (like the waiters of {@code SimpleConnectionPool}).
 *   The deadline is a Netty timer of the event loop of the request (sub-millisecond), cancelled when it's served.</li>
 *   <li><b>Creation affinity</b> ({@link #connectionContext}): when the pool isn't full, the connection opens on
 *   the event loop of the request while that loop has fewer than {@code maxSize / number of event loops}
 *   connections, else on an event loop that has fewer than that.</li>
 * </ul>
 * The state is only modified by actions, serialized by the executor, so a waiter is in at most one queue,
 * and is served at most once: the deadline timer only submits an action, which does nothing
 * if the waiter has been served or cancelled in the meantime.
 */
public class LocalWaitConnectionPool<C> implements ConnectionPool<C> {

  private static final Future POOL_CLOSED = Future.failedFuture("Pool closed");

  /**
   * Starts the timers of the local waits.
   */
  public interface Scheduler {

    /**
     * @return a way to cancel the timer
     */
    Runnable schedule(EventLoop eventLoop, long delayNanos, Runnable task);
  }

  /**
   * Netty timers: they run on the event loop of the waiter, with a sub-millisecond resolution as far as
   * the event loop selector goes.
   */
  public static final Scheduler NETTY_SCHEDULER = (eventLoop, delayNanos, task) -> {
    var future = eventLoop.schedule(task, delayNanos, TimeUnit.NANOSECONDS);
    return () -> future.cancel(false);
  };

  /**
   * @param localWaitNanos how long a request waits for a connection of its event loop, 0 to not wait (steal at once)
   * @param depth the local wait is skipped when the event loop has more than {@code depth} &times; its number of
   * connections local waiters, 0 to always wait
   * @param creationAffinity open the new connections on the event loop of the request, up to
   * {@code maxSize / eventLoops.size()} for each event loop
   * @param eventLoops the event loops that share the connections, in the order of the round-robin of
   * the context provider (empty: the plain pool opens each connection on the event loop of the request)
   * @param scheduler starts the timers
   * @param stats receives what happens to the waiters
   */
  public record Options(long localWaitNanos, int depth, boolean creationAffinity, List<EventLoop> eventLoops,
                        Scheduler scheduler, LocalWaitStats stats) {
  }

  /**
   * Select the first available available connection with the same event loop.
   */
  private static final BiFunction<PoolWaiter, List<PoolConnection>, PoolConnection> SAME_EVENT_LOOP_SELECTOR = (waiter, list) -> {
    int size = list.size();
    for (int i = 0;i < size;i++) {
      PoolConnection slot = list.get(i);
      if (slot.context().nettyEventLoop() == waiter.context().nettyEventLoop() && slot.available() > 0) {
        return slot;
      }
    }
    return null;
  };

  /**
   * Select the first available connection.
   */
  private static final BiFunction<PoolWaiter, List<PoolConnection>, PoolConnection> FIRST_AVAILABLE_SELECTOR = (waiter, list) -> {
    int size = list.size();
    for (int i = 0;i < size;i++) {
      PoolConnection slot = list.get(i);
      if (slot.available() > 0) {
        return slot;
      }
    }
    return null;
  };

  /**
   * What the pool knows about an event loop: its connections, and the requests waiting for one of them.
   */
  private static final class Loop<C> {
    final EventLoop eventLoop;
    final Waiters<C> localWaiters = new Waiters<>();
    int connections;

    Loop(EventLoop eventLoop) {
      this.eventLoop = eventLoop;
    }
  }

  /**
   * A slot for a connection.
   */
  static class Slot<C> implements PoolConnector.Listener, PoolConnection<C> {

    private final LocalWaitConnectionPool<C> pool;
    private final ContextInternal context;
    private final Loop<C> loop;
    private final Promise<C> result;
    private PoolWaiter<C> initiator;
    private C connection;    // The actual connection, might be null
    private int index;       // The index in the pool slots array
    private int usage;    // The number of times this connection is acquired
    private long concurrency; // The total number of times the connection can be acquired
    private int capacity;      // The connection capacity

    public Slot(LocalWaitConnectionPool<C> pool, ContextInternal context, int index, int capacity) {
      this.pool = pool;
      this.context = context;
      this.loop = pool.loop(context.nettyEventLoop());
      this.loop.connections++;
      this.connection = null;
      this.usage = 0;
      this.index = index;
      this.capacity = capacity;
      this.result = context.promise();
    }

    @Override
    public void onRemove() {
      pool.remove(this);
    }

    @Override
    public void onConcurrencyChange(long concurrency) {
      pool.setConcurrency(this, concurrency);
    }

    @Override
    public ContextInternal context() {
      return context;
    }

    @Override
    public C get() {
      return connection;
    }

    @Override
    public int usage() {
      return usage;
    }

    @Override
    public long available() {
      return concurrency - usage;
    }

    @Override
    public long concurrency() {
      return concurrency;
    }
  }

  private final PoolConnector<C> connector;
  private final int maxWaiters;
  private final int maxCapacity;
  private final int[] capacityFactors;
  private final Executor<LocalWaitConnectionPool<C>> sync;
  private final ListImpl list = new ListImpl();
  private final Options options;
  private final LocalWaitStats stats;

  // Whether the pool is closed
  private boolean closed;

  // Selectors
  private BiFunction<PoolWaiter<C>, List<PoolConnection<C>>, PoolConnection<C>> selector;
  private Function<ContextInternal, ContextInternal> contextProvider;
  private BiFunction<PoolWaiter<C>, List<PoolConnection<C>>, PoolConnection<C>> fallbackSelector;

  // Connection state
  private final Slot<C>[] slots;    // The pool connections, this array is not sparse
  private int size;                 // The number of non null slots
  private int capacity;             // The pool capacity

  // The event loops
  private final Map<EventLoop, Loop<C>> loops = new IdentityHashMap<>();

  // The waiters: each one is in the local queue of its event loop (still waiting for a local connection),
  // or in the queue of the waiters whose local wait is over (or that don't wait for a local connection)
  private final Waiters<C> expired = new Waiters<>();
  private int next;
  private int waiterCount;
  private long nextSequence;
  private int requests;

  public LocalWaitConnectionPool(PoolConnector<C> connector, int[] maxSizes, int maxWaiters, Options options) {

    int[] capacities = new int[maxSizes.length];
    int maxCapacity = 1;
    int numSlots = 0;
    for (int i = 0;i < maxSizes.length;i++) {
      int maxSize = maxSizes[i];
      if (maxSize < 1) {
        throw new IllegalArgumentException();
      }
      maxCapacity *= maxSize;
      numSlots = Math.max(numSlots, maxSize);
    }
    for (int i = 0;i < maxSizes.length;i++) {
      capacities[i] = maxCapacity / maxSizes[i];
    }

    this.capacityFactors = capacities;
    this.connector = connector;
    this.options = options;
    this.stats = options.stats();
    this.slots = new Slot[numSlots];
    this.size = 0;
    this.maxWaiters = maxWaiters;
    this.capacity = 0;
    this.maxCapacity = maxCapacity;
    this.sync = new CombinerExecutor<>(this);
    this.selector = (BiFunction) SAME_EVENT_LOOP_SELECTOR;
    this.fallbackSelector = (BiFunction) FIRST_AVAILABLE_SELECTOR;
    this.contextProvider = EVENT_LOOP_CONTEXT_PROVIDER;
  }

  @Override
  public ConnectionPool<C> connectionSelector(BiFunction<PoolWaiter<C>, List<PoolConnection<C>>, PoolConnection<C>> selector) {
    this.selector = selector;
    return this;
  }

  @Override
  public ConnectionPool<C> contextProvider(Function<ContextInternal, ContextInternal> contextProvider) {
    this.contextProvider = contextProvider;
    return this;
  }

  private void execute(Executor.Action<LocalWaitConnectionPool<C>> action) {
    sync.submit(action);
  }

  public int size() {
      return size;
  }

  // Only called by the actions
  private Loop<C> loop(EventLoop eventLoop) {
    Loop<C> loop = loops.get(eventLoop);
    if (loop == null) {
      loop = new Loop<>(eventLoop);
      loops.put(eventLoop, loop);
    }
    return loop;
  }

  /**
   * The context of a new connection, with {@link Options#creationAffinity()}: the event loop of the request while
   * it has fewer than its share of the connections ({@code maxSize / number of event loops}), else the next event
   * loop that has fewer than its share, else the one the context provider selects (round-robin).
   * Without it, always the one the context provider selects.
   */
  private ContextInternal connectionContext(ContextInternal requester) {
    List<EventLoop> eventLoops = options.eventLoops();
    if (options.creationAffinity() && !eventLoops.isEmpty()) {
      int share = slots.length / eventLoops.size();
      EventLoop own = requester.nettyEventLoop();
      EventLoop selected = null;
      if (eventLoops.contains(own) && loop(own).connections < share) {
        selected = own;
      } else {
        for (int i = 0;i < eventLoops.size() && selected == null;i++) {
          EventLoop candidate = eventLoops.get((next + i) % eventLoops.size());
          if (loop(candidate).connections < share) {
            selected = candidate;
          }
        }
        next++;
      }
      if (selected != null) {
        stats.affinityCreations.increment();
        return requester.owner().createEventLoopContext(selected, null, Thread.currentThread().getContextClassLoader());
      }
    }
    return contextProvider.apply(requester);
  }

  // Only called by the actions
  private void enqueue(Acquire<C> waiter, boolean local) {
    waiter.sequence = nextSequence++;
    waiterCount++;
    waiter.local = local;
    waiter.queue = local ? loop(waiter.context.nettyEventLoop()).localWaiters : expired;
    waiter.queue.addLast(waiter);
  }

  // Only called by the actions: the waiter whose local wait is over is the next in line, after the other ones
  private void enqueueExpired(Acquire<C> waiter, boolean first) {
    waiterCount++;
    waiter.local = false;
    waiter.queue = expired;
    if (first) {
      expired.addFirst(waiter);
    } else {
      expired.addLast(waiter);
    }
  }

  // Only called by the actions
  private boolean dequeue(Acquire<C> waiter) {
    Waiters<C> queue = waiter.queue;
    if (queue != null && queue.remove(waiter)) {
      waiter.queue = null;
      waiter.local = false;
      waiterCount--;
      return true;
    }
    return false;
  }

  /**
   * The waiter that gets a connection that just got free: the oldest local waiter of its event loop,
   * else the oldest waiter whose local wait is over, else none (a local waiter of another event loop
   * keeps waiting for a connection of its own).
   * <p>
   * Only called by the actions.
   */
  private Acquire<C> pollFor(Slot<C> slot) {
    Acquire<C> waiter = slot.loop.localWaiters.peek();
    if (waiter != null) {
      dequeue(waiter);
      stats.recordServedLocal(System.nanoTime() - waiter.enqueuedNanos);
      return waiter;
    }
    waiter = expired.peek();
    if (waiter != null) {
      dequeue(waiter);
      stats.servedAfterExpiry.increment();
      return waiter;
    }
    return null;
  }

  /**
   * The waiter that gets a connection that's about to be opened, because a connection was removed:
   * the oldest waiter, whatever its event loop.
   * <p>
   * Only called by the actions.
   */
  private Acquire<C> pollOldest() {
    Acquire<C> oldest = expired.peek();
    for (Loop<C> loop : loops.values()) {
      Acquire<C> candidate = loop.localWaiters.peek();
      if (candidate != null && (oldest == null || candidate.sequence < oldest.sequence)) {
        oldest = candidate;
      }
    }
    if (oldest != null) {
      dequeue(oldest);
    }
    return oldest;
  }

  // Only called by the actions
  private List<Acquire<C>> clearWaiters() {
    List<Acquire<C>> all = new ArrayList<>(waiterCount);
    for (Loop<C> loop : loops.values()) {
      all.addAll(loop.localWaiters.clear());
    }
    all.addAll(expired.clear());
    for (Acquire<C> waiter : all) {
      waiter.queue = null;
      waiter.local = false;
    }
    waiterCount = 0;
    return all;
  }

  public void connect(Slot<C> slot, PoolWaiter<C> waiter) {
    slot.initiator = waiter;
    connector.connect(slot.context, slot, ar -> {
      slot.initiator = null;
      if (ar.succeeded()) {
        execute(new ConnectSuccess<>(slot, ar.result(), waiter));
      } else {
        execute(new ConnectFailed<>(slot, ar.cause(), waiter));
      }
    });
  }

  /**
   * Gives a connection to waiters, and cancels their deadline, when the pool has updated its state.
   */
  private static final class Emit<C> extends Task {

    private final LeaseImpl<C> lease;
    private final Acquire<C> waiter;

    Emit(LeaseImpl<C> lease, Acquire<C> waiter) {
      this.lease = lease;
      this.waiter = waiter;
    }

    @Override
    public void run() {
      waiter.cancelDeadline();
      lease.emit();
    }
  }

  private static class ConnectSuccess<C> implements Executor.Action<LocalWaitConnectionPool<C>> {

    private final Slot<C> slot;
    private final ConnectResult<C> result;
    private PoolWaiter<C> waiter;

    private ConnectSuccess(Slot<C> slot, ConnectResult<C> result, PoolWaiter<C> waiter) {
      this.slot = slot;
      this.result = result;
      this.waiter = waiter;
    }

    @Override
    public Task execute(LocalWaitConnectionPool<C> pool) {

      int capacity = pool.capacityFactors[(int)result.weight()];

      int initialCapacity = slot.capacity;
      slot.connection = result.connection();
      slot.concurrency = result.concurrency();
      slot.capacity = capacity;
      slot.usage = 0;
      pool.requests--;
      pool.capacity += (capacity - initialCapacity);
      if (pool.closed) {
        if (waiter.disposed) {
          waiter = null;
        } else {
          waiter.disposed = true;
        }
        return new Task() {
          @Override
          public void run() {
            if (waiter != null) {
              slot.context.emit(POOL_CLOSED, waiter.handler);
            }
            slot.result.complete(slot.connection);
          }
        };
      } else {
        long acquisitions = slot.concurrency;
        if (acquisitions == 0) {
          if (!waiter.disposed) {
            // The initiator goes first, but not first in line of the local waiters of an event loop
            pool.enqueueExpired((Acquire<C>) waiter, true);
          }
          return null;
        }
        LeaseImpl<C> lease;
        int c;
        if (waiter.disposed) {
          lease = null;
          c = 0;
        } else {
          lease = new LeaseImpl<>(slot, waiter.handler);
          c = 1;
          waiter.disposed = true;
          acquisitions--;
        }
        List<Emit<C>> leases = null;
        for (int i = 0;i < acquisitions;i++) {
          Acquire<C> w = pool.pollFor(slot);
          if (w == null) {
            break;
          }
          w.disposed = true;
          if (leases == null) {
            leases = new ArrayList<>();
          }
          leases.add(new Emit<>(new LeaseImpl<>(slot, w.handler), w));
          c++;
        }
        slot.usage = c;
        List<Emit<C>> extra = leases;
        return new Task() {
          @Override
          public void run() {
            if (lease != null) {
              lease.emit();
            }
            if (extra != null) {
              for (Emit<C> emit : extra) {
                emit.run();
              }
            }
            slot.result.complete(slot.connection);
          }
        };
      }
    }
  }

  private static class ConnectFailed<C> implements Executor.Action<LocalWaitConnectionPool<C>> {

    private final Slot<C> removed;
    private final Throwable cause;
    private PoolWaiter<C> waiter;

    public ConnectFailed(Slot<C> removed, Throwable cause, PoolWaiter<C> waiter) {
      this.removed = removed;
      this.cause = cause;
      this.waiter = waiter;
    }

    public Task execute(LocalWaitConnectionPool<C> pool) {
      pool.requests--;
      if (waiter.disposed) {
        waiter = null;
      } else {
        waiter.disposed = true;
      }
      Task task = new Task() {
        @Override
        public void run() {
          if (waiter != null) {
            Future<Lease<C>> waiterFailure;
            if (pool.closed) {
              waiterFailure = POOL_CLOSED;
            } else {
              waiterFailure = Future.failedFuture(cause);
            }
            removed.context.emit(waiterFailure, waiter.handler);
          }
          removed.result.fail(cause);
        }
      };
      if (!pool.closed) {
        Task removeTask = new Remove<>(removed).execute(pool);
        if (removeTask != null) {
          removeTask.next(task);
          task = removeTask;
        }
      }
      return task;
    }
  }

  private static class Remove<C> implements Executor.Action<LocalWaitConnectionPool<C>> {

    protected final Slot<C> removed;

    private Remove(Slot<C> removed) {
      this.removed = removed;
    }

    @Override
    public Task execute(LocalWaitConnectionPool<C> pool) {
      if (pool.closed || pool.slots[removed.index] != removed) {
        return null;
      }
      int w = removed.capacity;
      removed.usage = 0;
      removed.concurrency = 0;
      removed.connection = null;
      removed.capacity = 0;
      removed.loop.connections--;
      Acquire<C> waiter = pool.pollOldest();
      if (waiter != null) {
        ContextInternal connectionContext = pool.connectionContext(waiter.context);
        Slot<C> slot = new Slot<>(pool, connectionContext, removed.index, waiter.capacity);
        pool.capacity -= w;
        pool.capacity += waiter.capacity;
        pool.slots[removed.index] = slot;
        pool.requests++;
        return new Task() {
          @Override
          public void run() {
            waiter.cancelDeadline();
            if (waiter.listener != null) {
              waiter.listener.onConnect(waiter);
            }
            pool.connect(slot, waiter);
          }
        };
      } else if (pool.size > 1) {
        Slot<C> tmp = pool.slots[pool.size - 1];
        tmp.index = removed.index;
        pool.slots[removed.index] = tmp;
        pool.slots[pool.size - 1] = null;
        pool.size--;
        pool.capacity -= w;
        return null;
      } else {
        pool.slots[0] = null;
        pool.size--;
        pool.capacity -= w;
        return null;
      }
    }
  }

  private static class SetConcurrency<C> implements Executor.Action<LocalWaitConnectionPool<C>> {

    private final Slot<C> slot;
    private final long concurrency;

    SetConcurrency(Slot<C> slot, long concurrency) {
      this.slot = slot;
      this.concurrency = concurrency;
    }

    @Override
    public Task execute(LocalWaitConnectionPool<C> pool) {
      if (slot.connection != null) {
        long diff = concurrency - slot.concurrency;
        slot.concurrency += diff;
        if (diff > 0) {
          List<Emit<C>> extra = null;
          while (slot.concurrency > slot.usage) {
            Acquire<C> w = pool.pollFor(slot);
            if (w == null) {
              break;
            }
            w.disposed = true;
            if (extra == null) {
              extra = new ArrayList<>();
            }
            extra.add(new Emit<>(new LeaseImpl<>(slot, w.handler), w));
            slot.usage++;
          }
          if (extra != null) {
            List<Emit<C>> emits = extra;
            return new Task() {
              @Override
              public void run() {
                for (Emit<C> emit : emits) {
                  emit.run();
                }
              }
            };
          } else {
            return null;
          }
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
  }

  private void setConcurrency(Slot<C> slot, long concurrency) {
    execute(new SetConcurrency<>(slot, concurrency));
  }

  private void remove(Slot<C> removed) {
    execute(new Remove<>(removed));
  }

  private static class Evict<C> implements Executor.Action<LocalWaitConnectionPool<C>> {

    private final Predicate<C> predicate;
    private final Handler<AsyncResult<List<C>>> handler;

    public Evict(Predicate<C> predicate, Handler<AsyncResult<List<C>>> handler) {
      this.predicate = predicate;
      this.handler = handler;
    }

    @Override
    public Task execute(LocalWaitConnectionPool<C> pool) {
      if (pool.closed) {
        return new Task() {
          @Override
          public void run() {
            handler.handle(POOL_CLOSED);
          }
        };
      }
      List<C> res = new ArrayList<>();
      List<Slot<C>> removed = new ArrayList<>();
      for (int i = pool.size - 1;i >= 0;i--) {
        Slot<C> slot = pool.slots[i];
        if (slot.connection != null && slot.usage == 0 && predicate.test(slot.connection)) {
          removed.add(slot);
          res.add(slot.connection);
        }
      }
      Task head = new Task() {
        @Override
        public void run() {
          handler.handle(Future.succeededFuture(res));
        }
      };
      Task tail = head;
      for (Slot<C> slot : removed) {
        Task next = new Remove<>(slot).execute(pool);
        if (next != null) {
          tail.next(next);
          tail = next;
        }
      }
      return head;
    }
  }

  @Override
  public void evict(Predicate<C> predicate, Handler<AsyncResult<List<C>>> handler) {
    execute(new Evict<>(predicate, handler));
  }

  private static class Acquire<C> extends PoolWaiter<C> implements Executor.Action<LocalWaitConnectionPool<C>> {

    // The queue the waiter is in, null if it isn't queued
    Waiters<C> queue;
    // Whether the queue is the local queue of its event loop: the waiter is still waiting for a local connection
    boolean local;
    // The order of arrival, for the waiters of all the event loops
    long sequence;
    // When the local wait started, and when it ends
    long enqueuedNanos;
    long deadlineNanos;
    // The timer of the local wait, started after the action: it can be cancelled before that
    private Runnable deadline;
    private boolean deadlineCancelled;

    public Acquire(ContextInternal context, PoolWaiter.Listener<C> listener, int capacity, Handler<AsyncResult<Lease<C>>> handler) {
      super(listener, context, capacity, handler);
    }

    synchronized void startDeadline(Runnable cancel) {
      if (deadlineCancelled) {
        cancel.run();
      } else {
        deadline = cancel;
      }
    }

    synchronized void cancelDeadline() {
      deadlineCancelled = true;
      if (deadline != null) {
        deadline.run();
        deadline = null;
      }
    }

    @Override
    public Task execute(LocalWaitConnectionPool<C> pool) {
      if (pool.closed) {
        return new Task() {
          @Override
          public void run() {
            context.emit(POOL_CLOSED, handler);
          }
        };
      }

      // 1. Try reuse a existing connection with the same context
      Slot<C> slot1 = (Slot<C>) pool.selector.apply(this, pool.list);
      if (slot1 != null) {
        slot1.usage++;
        LeaseImpl<C> lease = new LeaseImpl<>(slot1, handler);
        return new Task() {
          @Override
          public void run() {
            lease.emit();
          }
        };
      }

      // 2. Try create connection
      if (pool.capacity < pool.maxCapacity) {
        pool.capacity += capacity;
        ContextInternal connectionContext = pool.connectionContext(context);
        Slot<C> slot2 = new Slot<>(pool, connectionContext, pool.size, capacity);
        pool.slots[pool.size++] = slot2;
        pool.requests++;
        return new Task() {
          @Override
          public void run() {
            if (listener != null) {
              listener.onConnect(Acquire.this);
            }
            pool.connect(slot2, Acquire.this);
          }
        };
      }

      // 3. Wait for a connection of the same event loop, for a while,
      // unless the event loop has no connection or too many waiters
      boolean localWait = pool.options.localWaitNanos() > 0;
      if (localWait) {
        Loop<C> loop = pool.loop(context.nettyEventLoop());
        if (loop.connections == 0) {
          pool.stats.noLocalConnection.increment();
          localWait = false;
        } else if (pool.options.depth() > 0 && loop.localWaiters.size() > (long) pool.options.depth() * loop.connections) {
          pool.stats.depthShortcuts.increment();
          localWait = false;
        }
      }

      // 4. Try use another context
      if (!localWait) {
        Slot<C> slot3 = (Slot<C>) pool.fallbackSelector.apply(this, pool.list);
        if (slot3 != null) {
          slot3.usage++;
          LeaseImpl<C> lease = new LeaseImpl<>(slot3, handler);
          return new Task() {
            @Override
            public void run() {
              lease.emit();
            }
          };
        }
      }

      // 5. Fall in waiters list
      if (pool.maxWaiters == -1 || (pool.waiterCount + pool.requests) < pool.maxWaiters) {
        boolean waitLocally = localWait;
        pool.enqueue(this, waitLocally);
        if (waitLocally) {
          pool.stats.localWaits.increment();
          enqueuedNanos = System.nanoTime();
          deadlineNanos = enqueuedNanos + pool.options.localWaitNanos();
        }
        if (listener != null || waitLocally) {
          return new Task() {
            @Override
            public void run() {
              if (listener != null) {
                listener.onEnqueue(Acquire.this);
              }
              if (waitLocally) {
                // Not in the action: it only updates the state
                startDeadline(pool.options.scheduler().schedule(context.nettyEventLoop(), pool.options.localWaitNanos(),
                  () -> pool.execute(new Expire<>(Acquire.this))));
              }
            }
          };
        } else {
          return null;
        }
      } else {
        return new Task() {
          @Override
          public void run() {
            context.emit(Future.failedFuture(new ConnectionPoolTooBusyException("Connection pool reached max wait queue size of " + pool.maxWaiters)), handler);
          }
        };
      }
    }
  }

  /**
   * The local wait of a waiter is over, run by its deadline timer. Does nothing if the waiter has been served or
   * cancelled since.
   */
  private static class Expire<C> implements Executor.Action<LocalWaitConnectionPool<C>> {

    private final Acquire<C> waiter;

    Expire(Acquire<C> waiter) {
      this.waiter = waiter;
    }

    @Override
    public Task execute(LocalWaitConnectionPool<C> pool) {
      if (pool.closed || waiter.disposed || !waiter.local) {
        return null;
      }
      pool.dequeue(waiter);
      pool.stats.recordExpiry(System.nanoTime() - waiter.deadlineNanos);
      Slot<C> slot = (Slot<C>) pool.fallbackSelector.apply(waiter, pool.list);
      if (slot != null) {
        slot.usage++;
        waiter.disposed = true;
        pool.stats.expiredStolen.increment();
        return new Emit<>(new LeaseImpl<>(slot, waiter.handler), waiter);
      }
      // Nothing is free: wait for the first connection released by any event loop, after the other waiters
      pool.stats.expiredQueued.increment();
      pool.enqueueExpired(waiter, false);
      return null;
    }
  }

  @Override
  public void acquire(ContextInternal context, PoolWaiter.Listener<C> listener, int kind, Handler<AsyncResult<Lease<C>>> handler) {
    execute(new Acquire<>(context, listener, capacityFactors[kind], handler));
  }

  @Override
  public void acquire(ContextInternal context, int kind, Handler<AsyncResult<Lease<C>>> handler) {
    acquire(context, PoolWaiter.NULL_LISTENER, kind, handler);
  }

  @Override
  public void cancel(PoolWaiter<C> waiter, Handler<AsyncResult<Boolean>> handler) {
    execute(new Cancel<>(waiter, handler));
  }

  private static class Cancel<C> extends Task implements Executor.Action<LocalWaitConnectionPool<C>> {

    private final PoolWaiter<C> waiter;
    private final Handler<AsyncResult<Boolean>> handler;
    private boolean cancelled;

    public Cancel(PoolWaiter<C> waiter, Handler<AsyncResult<Boolean>> handler) {
      this.waiter = waiter;
      this.handler = handler;
    }

    @Override
    public Task execute(LocalWaitConnectionPool<C> pool) {
      if (pool.closed) {
        return new Task() {
          @Override
          public void run() {
            handler.handle(POOL_CLOSED);
          }
        };
      }
      if (pool.dequeue((Acquire<C>) waiter)) {
        cancelled = true;
        waiter.disposed = true;
      } else if (!waiter.disposed) {
        waiter.disposed = true;
        cancelled = true;
      } else {
        cancelled = false;
      }
      return this;
    }

    @Override
    public void run() {
      ((Acquire<C>) waiter).cancelDeadline();
      handler.handle(Future.succeededFuture(cancelled));
    }
  }

  static class LeaseImpl<C> implements Lease<C> {

    private final Handler<AsyncResult<Lease<C>>> handler;
    private final Slot<C> slot;
    private final C connection;
    private boolean recycled;

    public LeaseImpl(Slot<C> slot, Handler<AsyncResult<Lease<C>>> handler) {
      this.handler = handler;
      this.slot = slot;
      this.connection = slot.connection;
    }

    @Override
    public C get() {
      return connection;
    }

    @Override
    public void recycle() {
      slot.pool.recycle(this);
    }

    void emit() {
      slot.context.emit(Future.succeededFuture(this), handler);
    }
  }

  private static class Recycle<C> implements Executor.Action<LocalWaitConnectionPool<C>> {

    private final Slot<C> slot;

    public Recycle(Slot<C> slot) {
      this.slot = slot;
    }

    @Override
    public Task execute(LocalWaitConnectionPool<C> pool) {
      if (!pool.closed && slot.connection != null) {
        Acquire<C> waiter;
        if (slot.usage <= slot.concurrency && (waiter = pool.pollFor(slot)) != null) {
          waiter.disposed = true;
          return new Emit<>(new LeaseImpl<>(slot, waiter.handler), waiter);
        } else {
          slot.usage--;
        }
      }
      return null;
    }
  }

  private void recycle(LeaseImpl<C> lease) {
    if (lease.recycled) {
      throw new IllegalStateException("Attempt to recycle more than permitted");
    }
    lease.recycled = true;
    execute(new Recycle<>(lease.slot));
  }

  public int waiters() {
    return waiterCount;
  }

  public int capacity() {
    return capacity;
  }

  @Override
  public int requests() {
    return requests;
  }

  private static class Close<C> implements Executor.Action<LocalWaitConnectionPool<C>> {

    private final Handler<AsyncResult<List<Future<C>>>> handler;

    private Close(Handler<AsyncResult<List<Future<C>>>> handler) {
      this.handler = handler;
    }

    @Override
    public Task execute(LocalWaitConnectionPool<C> pool) {
      if (pool.closed) {
        return new Task() {
          @Override
          public void run() {
            handler.handle(POOL_CLOSED);
          }
        };
      }
      pool.closed = true;
      List<Acquire<C>> queued = pool.clearWaiters();
      List<PoolWaiter<C>> waiters = new ArrayList<>(queued);
      List<Future<C>> list = new ArrayList<>();
      for (int i = 0;i < pool.size;i++) {
        Slot<C> slot = pool.slots[i];
        pool.slots[i] = null;
        PoolWaiter<C> waiter = slot.initiator;
        if (waiter != null) {
          waiters.add(slot.initiator);
          slot.initiator.disposed = true;
          slot.initiator = null;
        }
        pool.capacity -= slot.capacity;
        list.add(slot.result.future());
      }
      pool.size = 0;
      return new Task() {
        @Override
        public void run() {
          queued.forEach(Acquire::cancelDeadline);
          waiters.forEach(w -> w.context.emit(POOL_CLOSED, w.handler));
          handler.handle(Future.succeededFuture(list));
        }
      };
    }
  }

  @Override
  public void close(Handler<AsyncResult<List<Future<C>>>> handler) {
    execute(new Close<>(handler));
  }

  /**
   * A queue of waiters, ordered by arrival: a waiter is in at most one queue.
   */
  private static class Waiters<C> implements Iterable<Acquire<C>> {

    private final Acquire<C> head;
    private int size;

    public Waiters() {
      head = new Acquire<>(null, null, 0, null);
      head.next = head.prev = head;
    }

    Acquire<C> peek() {
      return head.next == head ? null : (Acquire<C>) head.next;
    }

    void addLast(Acquire<C> node) {
      if (node.queued) {
        throw new IllegalStateException();
      }
      node.queued = true;
      node.prev = head.prev;
      node.next = head;
      head.prev.next = node;
      head.prev = node;
      size++;
    }

    void addFirst(Acquire<C> node) {
      if (node.queued) {
        throw new IllegalStateException();
      }
      node.queued = true;
      node.prev = head;
      node.next = head.next;
      head.next.prev = node;
      head.next = node;
      size++;
    }

    boolean remove(Acquire<C> node) {
      if (!node.queued) {
        return false;
      }
      node.next.prev = node.prev;
      node.prev.next = node.next;
      node.next = node.prev = null;
      node.queued = false;
      size--;
      return true;
    }

    List<Acquire<C>> clear() {
      List<Acquire<C>> lst = new ArrayList<>(size);
      this.forEach(lst::add);
      for (Acquire<C> waiter : lst) {
        waiter.queued = false;
        waiter.next = waiter.prev = null;
      }
      size = 0;
      head.next = head.prev = head;
      return lst;
    }

    int size() {
      return size;
    }

    @Override
    public Iterator<Acquire<C>> iterator() {
      return new Iterator<Acquire<C>>() {
        PoolWaiter<C> current = head;
        @Override
        public boolean hasNext() {
          return current.next != head;
        }
        @Override
        public Acquire<C> next() {
          if (current.next == head) {
            throw new NoSuchElementException();
          }
          try {
            return (Acquire<C>) current.next;
          } finally {
            current = current.next;
          }
        }
      };
    }
  }

  class ListImpl extends AbstractList<PoolConnection<C>> {
    @Override
    public PoolConnection<C> get(int index) {
      return slots[index];
    }
    @Override
    public int size() {
      return size;
    }
  }
}

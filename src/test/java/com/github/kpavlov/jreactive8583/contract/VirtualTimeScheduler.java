package com.github.kpavlov.jreactive8583.contract;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A deterministic {@link ScheduledExecutorService} driven by a virtual clock.
 * Tasks only run when the clock is advanced via {@link #advanceBy}, and every
 * scheduling decision is recorded in an event timeline, so a scenario can be
 * replayed with an identical event sequence.
 */
public final class VirtualTimeScheduler implements ScheduledExecutorService {

    private final List<String> timeline = new ArrayList<>();
    private final PriorityQueue<VirtualScheduledFuture<?>> queue = new PriorityQueue<>();
    private long nowNanos;
    private long sequence;
    private boolean shutdown;

    public List<String> timeline() {
        return List.copyOf(timeline);
    }

    public void record(final String event) {
        timeline.add(event);
    }

    public int pendingCount() {
        return queue.size();
    }

    /** Advances the virtual clock, running every task that becomes due, in deadline order. */
    public void advanceBy(final long amount, final TimeUnit unit) {
        final long target = nowNanos + unit.toNanos(amount);
        runDue(target);
        nowNanos = target;
        timeline.add("now=" + nowNanos);
    }

    private void runDue(final long targetNanos) {
        VirtualScheduledFuture<?> next;
        while ((next = queue.peek()) != null && next.deadlineNanos <= targetNanos) {
            queue.poll();
            nowNanos = Math.max(nowNanos, next.deadlineNanos);
            if (next.cancelled) {
                timeline.add("cancelled#" + next.id);
                continue;
            }
            next.run();
            timeline.add("fired#" + next.id + "@" + next.deadlineNanos);
        }
    }

    private void checkOpen() {
        if (shutdown) {
            throw new IllegalStateException("scheduler is shut down");
        }
    }

    @Override
    public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit) {
        checkOpen();
        final var task = new VirtualScheduledFuture<>(sequence++, nowNanos + unit.toNanos(delay), callable);
        queue.add(task);
        timeline.add("scheduled#" + task.id + "@" + task.deadlineNanos);
        return task;
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
        return schedule(Executors.callable(command, null), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
        final Runnable command, final long initialDelay, final long period, final TimeUnit unit
    ) {
        return schedule(new ReschedulingRunnable(command, period, unit), initialDelay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
        final Runnable command, final long initialDelay, final long delay, final TimeUnit unit
    ) {
        return schedule(new ReschedulingRunnable(command, delay, unit), initialDelay, unit);
    }

    @Override
    public void execute(final Runnable command) {
        checkOpen();
        command.run();
        timeline.add("execute");
    }

    @Override
    public java.util.concurrent.Future<?> submit(final Runnable task) {
        return submit(Executors.callable(task, null));
    }

    @Override
    public <T> java.util.concurrent.Future<T> submit(final Runnable task, final T result) {
        return submit(Executors.callable(task, result));
    }

    @Override
    public <T> java.util.concurrent.Future<T> submit(final Callable<T> task) {
        checkOpen();
        try {
            timeline.add("submit");
            return java.util.concurrent.CompletableFuture.completedFuture(task.call());
        } catch (final Exception e) {
            return java.util.concurrent.CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
        queue.clear();
        timeline.add("shutdown");
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && queue.isEmpty();
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) {
        return isTerminated();
    }

    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) {
        final List<Future<T>> results = new ArrayList<>();
        for (final Callable<T> task : tasks) {
            results.add(submit(task));
        }
        return results;
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit
    ) {
        return invokeAll(tasks);
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        return invokeAll(tasks).get(0).get();
    }

    @Override
    public <T> T invokeAny(
        final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit
    ) throws InterruptedException, ExecutionException, TimeoutException {
        return invokeAny(tasks);
    }

    private final class ReschedulingRunnable implements Runnable {
        private final Runnable delegate;
        private final long delay;
        private final TimeUnit unit;

        private ReschedulingRunnable(final Runnable delegate, final long delay, final TimeUnit unit) {
            this.delegate = delegate;
            this.delay = delay;
            this.unit = unit;
        }

        @Override
        public void run() {
            delegate.run();
            if (!shutdown) {
                schedule(this, delay, unit);
            }
        }
    }

    private final class VirtualScheduledFuture<V> implements ScheduledFuture<V> {
        private final long id;
        private final long deadlineNanos;
        private final Callable<V> callable;
        private boolean done;
        private boolean cancelled;
        private V result;

        private VirtualScheduledFuture(final long id, final long deadlineNanos, final Callable<V> callable) {
            this.id = id;
            this.deadlineNanos = deadlineNanos;
            this.callable = callable;
        }

        private void run() {
            if (done || cancelled) {
                return;
            }
            try {
                result = callable.call();
            } catch (final Exception e) {
                throw new IllegalStateException("virtual task #" + id + " failed", e);
            } finally {
                done = true;
            }
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            return unit.convert(deadlineNanos - nowNanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(final Delayed other) {
            final VirtualScheduledFuture<?> that = (VirtualScheduledFuture<?>) other;
            final int byDeadline = Long.compare(deadlineNanos, that.deadlineNanos);
            return byDeadline != 0 ? byDeadline : Long.compare(id, that.id);
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            cancelled = true;
            return queue.remove(this);
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public V get() throws ExecutionException {
            run();
            return result;
        }

        @Override
        public V get(final long timeout, final TimeUnit unit) throws ExecutionException, TimeoutException {
            return get();
        }
    }
}

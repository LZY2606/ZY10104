package com.github.kpavlov.jreactive8583.contract.support;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-threaded virtual clock used to make reconnect timing fully deterministic.
 *
 * <p>Nothing runs on its own: tasks are enqueued with a virtual deadline and only
 * execute when the test calls {@link #advanceTimeBy(long, TimeUnit)} or
 * {@link #runDueTasks()}. Every scheduled/executed event is appended to a timeline
 * so two runs with the same seed can be compared byte-for-byte.</p>
 */
public final class DeterministicScheduler extends AbstractExecutorService
    implements ScheduledExecutorService {

    private final class Task<V> extends FutureTask<V>
        implements RunnableScheduledFuture<V> {
        private final long deadlineNanos;
        private final long sequence;
        private final String label;

        Task(final Callable<V> callable, final long deadlineNanos,
             final long sequence, final String label) {
            super(callable);
            this.deadlineNanos = deadlineNanos;
            this.sequence = sequence;
            this.label = label;
        }

        Task(final Runnable runnable, final V result, final long deadlineNanos,
             final long sequence, final String label) {
            super(runnable, result);
            this.deadlineNanos = deadlineNanos;
            this.sequence = sequence;
            this.label = label;
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            return unit.convert(deadlineNanos - nowNanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(final Delayed other) {
            final var otherTask = (Task<?>) other;
            final var byDeadline = Long.compare(deadlineNanos, otherTask.deadlineNanos);
            return byDeadline != 0 ? byDeadline : Long.compare(sequence, otherTask.sequence);
        }

        @Override
        public boolean isPeriodic() {
            return false;
        }
    }

    private final PriorityQueue<Task<?>> queue = new PriorityQueue<>();
    private final List<String> timeline = new ArrayList<>();
    private final AtomicLong sequence = new AtomicLong();
    private long nowNanos;
    private boolean shutdown;

    public long currentTimeMillis() {
        return TimeUnit.NANOSECONDS.toMillis(nowNanos);
    }

    public List<String> timeline() {
        return timeline;
    }

    public int pendingCount() {
        return queue.size();
    }

    /** Advances virtual time and runs every task whose deadline is now due, in order. */
    public void advanceTimeBy(final long amount, final TimeUnit unit) {
        final long target = nowNanos + unit.toNanos(amount);
        while (!queue.isEmpty() && queue.peek().deadlineNanos <= target) {
            final var task = queue.poll();
            nowNanos = Math.max(nowNanos, task.deadlineNanos);
            timeline.add(currentTimeMillis() + "ms run:" + task.label);
            task.run();
        }
        nowNanos = target;
    }

    /** Runs all tasks due at the current virtual time. */
    public void runDueTasks() {
        while (!queue.isEmpty() && queue.peek().deadlineNanos <= nowNanos) {
            final var task = queue.poll();
            timeline.add(currentTimeMillis() + "ms run:" + task.label);
            task.run();
        }
    }

    private <V> ScheduledFuture<V> scheduleInternal(final Callable<V> callable,
                                                    final long delay,
                                                    final TimeUnit unit,
                                                    final String label) {
        if (shutdown) {
            throw new RejectedExecutionException("scheduler shut down");
        }
        final var deadline = nowNanos + unit.toNanos(delay);
        final var task = new Task<>(callable, deadline, sequence.incrementAndGet(), label);
        queue.add(task);
        timeline.add(currentTimeMillis() + "ms schedule:" + label
            + " delay=" + unit.toMillis(delay) + "ms");
        return task;
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, final long delay,
                                       final TimeUnit unit) {
        return scheduleInternal(() -> {
            command.run();
            return null;
        }, delay, unit, "runnable");
    }

    @Override
    public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay,
                                           final TimeUnit unit) {
        return scheduleInternal(callable, delay, unit, "reconnect");
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command,
                                                  final long initialDelay, final long period,
                                                  final TimeUnit unit) {
        throw new UnsupportedOperationException("periodic tasks are not used by reconnect logic");
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command,
                                                     final long initialDelay, final long delay,
                                                     final TimeUnit unit) {
        throw new UnsupportedOperationException("periodic tasks are not used by reconnect logic");
    }

    @Override
    public void shutdown() {
        shutdown = true;
        timeline.add(currentTimeMillis() + "ms shutdown");
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        final List<Runnable> remaining = new ArrayList<>();
        Task<?> task;
        while ((task = queue.poll()) != null) {
            remaining.add(task);
        }
        return remaining;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown;
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) {
        return shutdown;
    }

    @Override
    public void execute(final Runnable command) {
        timeline.add(currentTimeMillis() + "ms execute");
        command.run();
    }
}

package com.github.kpavlov.jreactive8583.contract.support;

import io.netty.channel.EventLoopGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Verifies that every resource owned by a contract fixture is released:
 * <ul>
 *     <li>event loop groups terminate within an explicit timeout;</li>
 *     <li>no event-loop thread attributed to the fixture stays alive;</li>
 *     <li>no tracked ByteBuf reference remains allocated.</li>
 * </ul>
 *
 * On failure it renders the ownership of each surviving resource, then still
 * attempts to forcefully terminate event loops so later rounds are not polluted.
 */
public final class ResourceAuditor {

    public static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final List<EventLoopGroup> eventLoopGroups = new ArrayList<>();
    private final List<TrackingByteBufAllocator> allocators = new ArrayList<>();
    private final List<String> threadNamePrefixes = new ArrayList<>();

    public ResourceAuditor trackEventLoop(final EventLoopGroup group, final String prefix) {
        eventLoopGroups.add(group);
        threadNamePrefixes.add(prefix);
        return this;
    }

    public ResourceAuditor trackAllocator(final TrackingByteBufAllocator allocator) {
        allocators.add(allocator);
        return this;
    }

    /**
     * Asserts all tracked resources are released. Always attempts cleanup, even
     * when earlier checks fail, and reports ownership of everything still alive.
     */
    public void assertAllReleased() {
        final List<String> failures = new ArrayList<>();

        // Request shutdown with a zero quiet period first so terminated groups are
        // reclaimed immediately rather than after Netty's default 2s delay.
        for (final var group : eventLoopGroups) {
            if (!group.isShuttingDown() && !group.isTerminated()) {
                group.shutdownGracefully(0, 0, TimeUnit.SECONDS);
            }
        }

        assertEventLoopsTerminated(failures);
        assertThreadsGone(failures);
        assertAllocatorsEmpty(failures);

        if (!failures.isEmpty()) {
            // Last-resort cleanup so a failure in one round does not leak into
            // subsequent rounds / tests.
            for (final var group : eventLoopGroups) {
                if (!group.isShuttingDown() && !group.isTerminated()) {
                    group.shutdownNow();
                }
            }
            throw new AssertionError(
                "Network contract resource leak detected:" + System.lineSeparator()
                    + String.join(System.lineSeparator(), failures));
        }
    }

    private void assertEventLoopsTerminated(final List<String> failures) {
        for (final var group : eventLoopGroups) {
            final boolean terminated;
            try {
                terminated = group
                    .awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                failures.add("Interrupted while awaiting termination of " + describe(group));
                continue;
            }
            if (!terminated) {
                failures.add("EventLoopGroup did not terminate within "
                    + SHUTDOWN_TIMEOUT_SECONDS + "s: " + describe(group)
                    + " (isShutdown=" + group.isShutdown()
                    + ", isTerminated=" + group.isTerminated() + ")");
            }
        }
    }

    private void assertThreadsGone(final List<String> failures) {
        // Give the JVM a short deterministic grace window to join terminated threads,
        // polling with an explicit deadline instead of sleeping.
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        List<Thread> survivors = findOwnedThreads();
        while (!survivors.isEmpty() && System.nanoTime() < deadline) {
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            survivors = findOwnedThreads();
        }
        for (final var thread : survivors) {
            failures.add("Surviving event-loop thread '" + thread.getName()
                + "' state=" + thread.getState()
                + " daemon=" + thread.isDaemon());
        }
    }

    private List<Thread> findOwnedThreads() {
        final var allThreads = Thread.getAllStackTraces().keySet();
        final List<Thread> owned = new ArrayList<>();
        for (final var thread : allThreads) {
            for (final var prefix : threadNamePrefixes) {
                if (thread.getName().startsWith(prefix)) {
                    owned.add(thread);
                    break;
                }
            }
        }
        return owned;
    }

    private void assertAllocatorsEmpty(final List<String> failures) {
        for (final var allocator : allocators) {
            if (allocator.liveBufferCount() != 0) {
                failures.add(allocator.describeLiveBuffers());
            }
        }
    }

    private String describe(final EventLoopGroup group) {
        return group.getClass().getSimpleName()
            + "[isShutdown=" + group.isShutdown()
            + ", isTerminated=" + group.isTerminated() + "]";
    }

}

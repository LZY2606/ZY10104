package com.github.kpavlov.jreactive8583.contract;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.awaitility.core.ConditionTimeoutException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

/**
 * Tracks Netty resources created by a test (event loop groups, channels) and
 * verifies after the test - including after a failed one - that event loops,
 * channels and allocator references are all released. Surviving resources are
 * reported together with the owner that registered them.
 */
public final class ResourceAudit {

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(15);

    private final Map<String, EventLoopGroup> groups = new LinkedHashMap<>();
    private final Map<String, Channel> channels = new LinkedHashMap<>();
    private final Set<String> baselineThreads;
    private final long baselineUsedDirectMemory;
    private final long baselineUsedHeapMemory;

    public ResourceAudit() {
        baselineThreads = nettyThreadNames();
        baselineUsedDirectMemory = usedDirectMemory();
        baselineUsedHeapMemory = usedHeapMemory();
    }

    public synchronized <T extends EventLoopGroup> T track(final String owner, final T group) {
        groups.put(owner, group);
        return group;
    }

    public synchronized <T extends Channel> T track(final String owner, final T channel) {
        channels.put(owner, channel);
        return channel;
    }

    /** Closes every tracked resource. Idempotent and safe to call after a failed test. */
    public synchronized void closeAll() {
        channels.values().forEach(channel ->
            channel.close().awaitUninterruptibly(SHUTDOWN_TIMEOUT.toMillis()));
        channels.clear();
        groups.values().forEach(group -> group.shutdownGracefully(0, 5, TimeUnit.SECONDS));
        groups.values().forEach(group -> {
            try {
                group.awaitTermination(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        groups.clear();
    }

    /** Fails with the ownership of every resource that is still alive. */
    public synchronized void assertAllReleased() {
        final List<String> survivors = new ArrayList<>();
        groups.forEach((owner, group) -> {
            if (!group.isTerminated()) {
                survivors.add("eventLoopGroup owner=" + owner + " -> " + group);
            }
        });
        channels.forEach((owner, channel) -> {
            if (channel.isOpen() || channel.isActive()) {
                survivors.add("channel owner=" + owner + " id=" + channel.id()
                    + " local=" + channel.localAddress() + " remote=" + channel.remoteAddress());
            }
        });
        awaitNoLingeringEventLoopThreads(survivors);
        final long directDelta = usedDirectMemory() - baselineUsedDirectMemory;
        final long heapDelta = usedHeapMemory() - baselineUsedHeapMemory;
        if (directDelta != 0 || heapDelta != 0) {
            survivors.add("allocator owner=PooledByteBufAllocator.DEFAULT"
                + " usedDirectDelta=" + directDelta + " usedHeapDelta=" + heapDelta);
        }
        if (!survivors.isEmpty()) {
            final String report = "Unreleased network resources (" + survivors.size() + "):"
                + survivors.stream().collect(Collectors.joining("\n - ", "\n - ", ""));
            System.err.println(report);
            throw new AssertionError(report);
        }
    }

    private void awaitNoLingeringEventLoopThreads(final List<String> survivors) {
        try {
            await().atMost(SHUTDOWN_TIMEOUT).pollInterval(Duration.ofMillis(50))
                .until(() -> lingeringEventLoopThreads().isEmpty());
        } catch (final ConditionTimeoutException e) {
            lingeringEventLoopThreads().forEach(thread ->
                survivors.add("eventLoopThread name=" + thread));
        }
    }

    private Set<String> lingeringEventLoopThreads() {
        final Set<String> current = nettyThreadNames();
        current.removeAll(baselineThreads);
        return current;
    }

    private static Set<String> nettyThreadNames() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(Thread::isAlive)
            .map(Thread::getName)
            .filter(name -> name.startsWith("nioEventLoopGroup"))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static long usedDirectMemory() {
        return PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();
    }

    private static long usedHeapMemory() {
        return PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory();
    }
}

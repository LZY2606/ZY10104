package com.github.kpavlov.jreactive8583.contract.support;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.TrackingByteBufs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ByteBuf allocator that keeps an attributed count of buffers it handed out and
 * that have not been deallocated.
 *
 * <p>Accounting is driven by Netty's package-private {@code deallocate()} hook
 * (invoked exactly when the reference count reaches zero on every release path),
 * so it is deterministic and does not depend on garbage collection, sampling or
 * wall-clock timing.</p>
 */
public final class TrackingByteBufAllocator extends AbstractByteBufAllocator {

    private static final class Allocation {
        private final String origin;
        private final long id;

        private Allocation(final String origin, final long id) {
            this.origin = origin;
            this.id = id;
        }
    }

    private final String owner;
    private final ConcurrentMap<Long, Allocation> live = new ConcurrentHashMap<>();
    private final AtomicInteger allocatedTotal = new AtomicInteger();
    private final AtomicLong sequence = new AtomicLong();

    public TrackingByteBufAllocator(final String owner) {
        // Heap-by-default: deterministic for loopback tests and avoids platform
        // specific direct-buffer internals while tracking deallocation.
        super(false);
        this.owner = owner;
    }

    @Override
    public boolean isDirectBufferPooled() {
        return false;
    }

    @Override
    protected ByteBuf newHeapBuffer(final int initialCapacity, final int maxCapacity) {
        return track(initialCapacity, maxCapacity);
    }

    @Override
    protected ByteBuf newDirectBuffer(final int initialCapacity, final int maxCapacity) {
        // Bounded by the heap allocator deliberately; see constructor note.
        return track(initialCapacity, maxCapacity);
    }

    private ByteBuf track(final int initialCapacity, final int maxCapacity) {
        final var id = sequence.incrementAndGet();
        allocatedTotal.incrementAndGet();
        live.put(id, new Allocation(Thread.currentThread().getName(), id));
        return TrackingByteBufs.trackedHeap(this, initialCapacity, maxCapacity,
            ignored -> live.remove(id));
    }

    public int liveBufferCount() {
        return live.size();
    }

    public int totalAllocated() {
        return allocatedTotal.get();
    }

    public String owner() {
        return owner;
    }

    /**
     * Renders ownership information about buffers that were allocated but never
     * deallocated. Returns an empty string when nothing leaks.
     */
    public String describeLiveBuffers() {
        if (live.isEmpty()) {
            return "";
        }
        final var builder = new StringBuilder();
        builder.append(owner).append(" has ").append(live.size())
            .append(" unreleased ByteBuf reference(s):");
        var shown = 0;
        for (final var allocation : live.values()) {
            if (shown++ >= 10) {
                break;
            }
            builder.append(System.lineSeparator())
                .append("  - allocation #").append(allocation.id)
                .append(" on thread '").append(allocation.origin).append('\'');
        }
        return builder.toString();
    }
}

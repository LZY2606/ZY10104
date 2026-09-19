package io.netty.buffer;

import java.util.function.LongConsumer;

/**
 * Test-only factory placed in {@code io.netty.buffer} so it can construct a
 * package-accessible tracked buffer subclass overriding Netty's
 * {@code deallocate()} hook (invoked exactly when the reference count reaches
 * zero, on every release path).
 */
public final class TrackingByteBufs {

    private TrackingByteBufs() {
    }

    public static ByteBuf trackedHeap(final ByteBufAllocator allocator,
                                      final int initialCapacity, final int maxCapacity,
                                      final LongConsumer onDeallocate) {
        return new TrackedHeapByteBuf(allocator, initialCapacity, maxCapacity, onDeallocate);
    }

    private static final class TrackedHeapByteBuf extends UnpooledUnsafeHeapByteBuf {
        private final LongConsumer onDeallocate;

        private TrackedHeapByteBuf(final ByteBufAllocator allocator, final int initialCapacity,
                                   final int maxCapacity, final LongConsumer onDeallocate) {
            super(allocator, initialCapacity, maxCapacity);
            this.onDeallocate = onDeallocate;
        }

        @Override
        protected void deallocate() {
            super.deallocate();
            onDeallocate.accept(1L);
        }
    }
}

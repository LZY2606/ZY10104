package com.github.kpavlov.jreactive8583.contract.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Seed-driven, deterministic description of how a faulty network delivers bytes:
 * a complete encoded frame (length prefix + body) is sliced into chunks
 * (length-prefix fragmentation), merged with a following frame (packet
 * coalescing / sticky packets) or truncated and followed by a channel close
 * (half frame).
 *
 * <p>The same seed always produces the same sequence of delivery events, so the
 * resulting timeline is reproducible across runs and machines.</p>
 */
public final class FaultScript {

    public enum EventType {
        DELIVER,
        HALF_FRAME,
        CLOSE_AFTER
    }

    public record Event(EventType type, byte[] data, String label) {
        static Event deliver(final byte[] data, final String label) {
            return new Event(EventType.DELIVER, data, label);
        }

        static Event halfFrame(final byte[] data, final String label) {
            return new Event(EventType.HALF_FRAME, data, label);
        }

        static Event closeAfter(final String label) {
            return new Event(EventType.CLOSE_AFTER, new byte[0], label);
        }
    }

    private final List<Event> events = new ArrayList<>();
    private final long seed;

    private FaultScript(final long seed) {
        this.seed = seed;
    }

    /**
     * Splits one or more complete encoded frames into deterministic inbound delivery
     * events, cutting each frame inside its length prefix.
     */
    @SafeVarargs
    public static FaultScript forFrames(final long seed, final byte[]... frames) {
        final var script = new FaultScript(seed);
        var frameIndex = 0;
        for (final var frame : frames) {
            final var prefix = "frame#" + frameIndex;
            // Deterministic split point inside the 2-byte length prefix: the first
            // length byte is delivered alone, then the second byte plus the body.
            final var splitAfter = 1 + new Random(seed).nextInt(1); // always 1
            final byte[] firstChunk = new byte[splitAfter];
            System.arraycopy(frame, 0, firstChunk, 0, splitAfter);
            script.events.add(Event.deliver(firstChunk, prefix + "-length-prefix-part1"));

            final byte[] secondChunk = new byte[frame.length - splitAfter];
            System.arraycopy(frame, splitAfter, secondChunk, 0, secondChunk.length);
            script.events.add(Event.deliver(secondChunk, prefix + "-length-prefix-part2+body"));
            frameIndex++;
        }
        return script;
    }

    /** Coalesces two frames into a single inbound chunk (sticky packet). */
    public static FaultScript stickyFrames(final long seed, final byte[] first,
                                           final byte[] second) {
        final var script = new FaultScript(seed);
        final var combined = new byte[first.length + second.length];
        System.arraycopy(first, 0, combined, 0, first.length);
        System.arraycopy(second, 0, combined, first.length, second.length);
        script.events.add(Event.deliver(combined, "sticky-two-frames"));
        return script;
    }

    /** Delivers only part of a frame and then schedules a channel close. */
    public static FaultScript halfFrameThenClose(final long seed, final byte[] frame) {
        final var script = new FaultScript(seed);
        final var cut = 1 + new Random(seed).nextInt(Math.max(1, frame.length - 1));
        final byte[] partial = new byte[cut];
        System.arraycopy(frame, 0, partial, 0, cut);
        script.events.add(Event.halfFrame(partial, "half-frame-" + cut + "-of-" + frame.length));
        script.events.add(Event.closeAfter("close-after-half-frame"));
        return script;
    }

    public List<Event> events() {
        return events;
    }

    public long seed() {
        return seed;
    }
}

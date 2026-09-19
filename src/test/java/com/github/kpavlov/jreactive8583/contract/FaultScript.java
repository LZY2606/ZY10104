package com.github.kpavlov.jreactive8583.contract;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Deterministic fault-injection script. Every decision is derived from the seed,
 * so the same seed always produces the same event timeline and the same byte
 * layout, which makes failures exactly reproducible.
 */
public final class FaultScript {

    private final Random random;
    private final List<String> timeline = new ArrayList<>();

    public FaultScript(final long seed) {
        this.random = new Random(seed);
        record("init seed=" + seed);
    }

    public List<String> timeline() {
        return List.copyOf(timeline);
    }

    public void record(final String event) {
        timeline.add(event);
    }

    /** Splits {@code frame} into a deterministic sequence of non-empty fragments. */
    public List<byte[]> fragment(final byte[] frame) {
        final int parts = 2 + random.nextInt(Math.min(4, frame.length));
        final List<Integer> cuts = new ArrayList<>();
        for (int i = 1; i < parts; i++) {
            cuts.add(1 + random.nextInt(frame.length - 1));
        }
        cuts.sort(Integer::compareTo);
        final List<byte[]> fragments = new ArrayList<>();
        int from = 0;
        for (final int cut : cuts) {
            if (cut > from) {
                fragments.add(Arrays.copyOfRange(frame, from, cut));
                from = cut;
            }
        }
        fragments.add(Arrays.copyOfRange(frame, from, frame.length));
        record("fragment parts=" + fragments.size()
            + " sizes=" + fragments.stream().map(f -> f.length).toList());
        return fragments;
    }

    /** Returns the first half of the frame, simulating a peer that vanishes mid-frame. */
    public byte[] halfFrame(final byte[] frame) {
        final int half = Math.max(1, frame.length / 2);
        record("halfFrame bytes=" + half);
        return Arrays.copyOfRange(frame, 0, half);
    }
}

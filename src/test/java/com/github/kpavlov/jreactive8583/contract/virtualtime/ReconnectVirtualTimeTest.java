package com.github.kpavlov.jreactive8583.contract.virtualtime;

import com.github.kpavlov.jreactive8583.client.Iso8583Client;
import com.github.kpavlov.jreactive8583.contract.support.DeterministicScheduler;
import com.github.kpavlov.jreactive8583.netty.pipeline.ReconnectOnCloseListener;
import io.netty.channel.ChannelFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Reconnect scheduling verified against a virtual clock. The exact same inputs
 * (reconnect interval, advance steps, seed) produce the exact same timeline, and
 * a reconnect already scheduled must not fire after a stop/disconnect request.
 */
@Tag("virtualtime")
class ReconnectVirtualTimeTest {

    private static final int INTERVAL_MILLIS = 100;

    private TestContext newContext() {
        @SuppressWarnings("unchecked")
        final Iso8583Client<com.solab.iso8583.IsoMessage> client = mock(Iso8583Client.class);
        when(client.connectAsync()).thenReturn(mock(ChannelFuture.class));
        final var scheduler = new DeterministicScheduler();
        final var listener =
            new ReconnectOnCloseListener(client, INTERVAL_MILLIS, scheduler);
        return new TestContext(client, scheduler, listener);
    }

    private record TestContext(
        Iso8583Client<com.solab.iso8583.IsoMessage> client,
        DeterministicScheduler scheduler,
        ReconnectOnCloseListener listener) {
    }

    @Test
    void reconnectFiresOnlyWhenVirtualIntervalElapses() {
        final var ctx = newContext();

        ctx.listener().requestReconnect();
        ctx.listener().scheduleReconnect();
        assertThat(ctx.scheduler().pendingCount()).isEqualTo(1);

        // Before the interval the queued reconnect must not run.
        ctx.scheduler().advanceTimeBy(INTERVAL_MILLIS - 1, TimeUnit.MILLISECONDS);
        verify(ctx.client(), never()).connectAsync();

        ctx.scheduler().advanceTimeBy(1, TimeUnit.MILLISECONDS);
        verify(ctx.client(), times(1)).connectAsync();
        assertThat(ctx.scheduler().pendingCount()).isZero();
    }

    @Test
    void multipleScheduledReconnectsFireInVirtualTimeOrder() {
        final var ctx = newContext();

        ctx.listener().requestReconnect();
        ctx.listener().scheduleReconnect();
        ctx.listener().scheduleReconnect();

        ctx.scheduler().advanceTimeBy(INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        verify(ctx.client(), times(2)).connectAsync();
        assertThat(ctx.scheduler().pendingCount())
            .as("no scheduled reconnect survives firing").isZero();
    }

    @Test
    void scheduledReconnectIsCancelledByStopRequestArrivingBeforeItFires() {
        final var ctx = newContext();

        ctx.listener().requestReconnect();
        ctx.listener().scheduleReconnect();

        // stop/reconnect race: the stop request lands while reconnect is already queued.
        ctx.listener().requestDisconnect();

        ctx.scheduler().advanceTimeBy(INTERVAL_MILLIS * 10L, TimeUnit.MILLISECONDS);
        verify(ctx.client(), never()).connectAsync();
        assertThat(ctx.scheduler().pendingCount()).isZero();
    }

    @Test
    void sameInputsProduceSameTimelineAcrossRuns() {
        // Two independent runs driven by the same seed must yield an identical timeline.
        final var first = runTimeline(new java.util.Random(424242L));
        final var second = runTimeline(new java.util.Random(424242L));
        assertThat(second).isEqualTo(first);
        assertThat(first).isNotEmpty();
    }

    @Test
    void twentyReconnectCyclesDoNotAccumulateScheduledTasksOrExtraConnections() {
        final var ctx = newContext();
        for (var cycle = 0; cycle < 20; cycle++) {
            ctx.listener().requestReconnect();
            ctx.listener().scheduleReconnect();
            // Exactly one reconnect is pending while waiting for the interval.
            assertThat(ctx.scheduler().pendingCount())
                .as("pending reconnects in cycle %d", cycle).isEqualTo(1);
            ctx.scheduler().advanceTimeBy(INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            // The queued reconnect fired and left no residual scheduled task.
            assertThat(ctx.scheduler().pendingCount())
                .as("residual tasks after cycle %d", cycle).isZero();
        }
        verify(ctx.client(), times(20)).connectAsync();
        assertThat(ctx.scheduler().pendingCount()).isZero();
    }

    private List<String> runTimeline(final java.util.Random random) {
        final var ctx = newContext();
        ctx.listener().requestReconnect();
        // Random but seed-driven number of failures before a disconnect request.
        final var failures = 1 + random.nextInt(4);
        for (var i = 0; i < failures; i++) {
            ctx.listener().scheduleReconnect();
            ctx.scheduler().advanceTimeBy(INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }
        ctx.listener().requestDisconnect();
        ctx.listener().scheduleReconnect();
        ctx.scheduler().advanceTimeBy(INTERVAL_MILLIS * 5L, TimeUnit.MILLISECONDS);
        return ctx.scheduler().timeline();
    }
}

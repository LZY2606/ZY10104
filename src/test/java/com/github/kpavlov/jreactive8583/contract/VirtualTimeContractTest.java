package com.github.kpavlov.jreactive8583.contract;

import com.github.kpavlov.jreactive8583.client.Iso8583Client;
import com.github.kpavlov.jreactive8583.netty.pipeline.IdleEventHandler;
import com.github.kpavlov.jreactive8583.netty.pipeline.ReconnectOnCloseListener;
import com.solab.iso8583.IsoMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Reconnect and idle behaviour verified against a virtual clock - no wall-clock
 * sleeps, identical event timeline for identical seeds.
 */
class VirtualTimeContractTest {

    @Test
    void reconnectIsScheduledOnCloseAndFiresAtConfiguredInterval() {
        final var scheduler = new VirtualTimeScheduler();
        final Iso8583Client<?> client = mock(Iso8583Client.class);
        final var connectAttempts = new AtomicInteger();
        when(client.connectAsync()).thenAnswer(invocation -> {
            scheduler.record("connectAsync#" + connectAttempts.incrementAndGet());
            return null;
        });
        final var listener = new ReconnectOnCloseListener(client, 100, scheduler);

        listener.operationComplete(closeEvent());

        scheduler.advanceBy(99, TimeUnit.MILLISECONDS);
        assertThat(connectAttempts).hasValue(0);

        scheduler.advanceBy(1, TimeUnit.MILLISECONDS);
        assertThat(connectAttempts).hasValue(1);

        scheduler.shutdown();
        assertThat(scheduler.pendingCount()).as("no reconnect left scheduled").isZero();
    }

    @Test
    void stopWinsRaceAgainstAlreadyScheduledReconnect() {
        final var scheduler = new VirtualTimeScheduler();
        final Iso8583Client<?> client = mock(Iso8583Client.class);
        final var listener = new ReconnectOnCloseListener(client, 100, scheduler);

        listener.operationComplete(closeEvent());
        listener.requestDisconnect();
        scheduler.advanceBy(10_000, TimeUnit.MILLISECONDS);

        verify(client, never()).connectAsync();
        assertThat(scheduler.pendingCount()).as("no reconnect left scheduled").isZero();
    }

    @Test
    void reconnectFiresWhenNotCancelledBeforeDeadline() {
        final var scheduler = new VirtualTimeScheduler();
        final Iso8583Client<?> client = mock(Iso8583Client.class);
        final var listener = new ReconnectOnCloseListener(client, 100, scheduler);

        listener.operationComplete(closeEvent());
        scheduler.advanceBy(100, TimeUnit.MILLISECONDS);

        verify(client, times(1)).connectAsync();
        scheduler.shutdown();
        assertThat(scheduler.pendingCount()).isZero();
    }

    @Test
    void sameSeedProducesIdenticalReconnectTimeline() {
        final List<String> first = simulateReconnectScenario(42L);
        final List<String> second = simulateReconnectScenario(42L);
        assertThat(first).isNotEmpty();
        assertThat(second).isEqualTo(first);
    }

    private List<String> simulateReconnectScenario(final long seed) {
        final var scheduler = new VirtualTimeScheduler();
        final Iso8583Client<?> client = mock(Iso8583Client.class);
        when(client.connectAsync()).thenAnswer(invocation -> {
            scheduler.record("connectAsync");
            return null;
        });
        final var listener = new ReconnectOnCloseListener(client, 100, scheduler);
        final var random = new Random(seed);
        for (int i = 0; i < 10; i++) {
            listener.operationComplete(closeEvent());
            scheduler.advanceBy(random.nextInt(250), TimeUnit.MILLISECONDS);
            if (random.nextBoolean()) {
                listener.requestDisconnect();
            } else {
                listener.requestReconnect();
            }
        }
        scheduler.shutdown();
        return scheduler.timeline();
    }

    @Test
    void heartbeatIsEmittedWhenIdleTimeoutElapsesInVirtualTime() throws Exception {
        final var messageFactory = TestMessages.messageFactory(com.github.kpavlov.jreactive8583.iso.MessageOrigin.ACQUIRER);
        final var channel = new EmbeddedChannel(
            new IdleStateHandler(0, 0, 5, TimeUnit.SECONDS),
            new IdleEventHandler<>(messageFactory));
        try {
            channel.freezeTime();
            channel.advanceTimeBy(4, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertThat((Object) channel.readOutbound()).as("no heartbeat before idle timeout").isNull();

            channel.advanceTimeBy(2, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            final Object heartbeat = channel.readOutbound();
            assertThat(heartbeat).isInstanceOf(IsoMessage.class);
            assertThat(((IsoMessage) heartbeat).getType()).isEqualTo(TestMessages.ECHO_REQUEST_MTI);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static ChannelFuture closeEvent() {
        final ChannelFuture future = mock(ChannelFuture.class);
        final Channel channel = mock(Channel.class);
        when(future.channel()).thenReturn(channel);
        return future;
    }
}

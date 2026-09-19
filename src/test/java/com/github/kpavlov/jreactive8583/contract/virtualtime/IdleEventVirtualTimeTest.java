package com.github.kpavlov.jreactive8583.contract.virtualtime;

import com.github.kpavlov.jreactive8583.contract.support.ContractMessageFactory;
import com.github.kpavlov.jreactive8583.netty.pipeline.IdleEventHandler;
import com.solab.iso8583.IsoMessage;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Idle heartbeat timing verified on an EmbeddedChannel virtual clock.
 * No real thread, port or wall-clock sleep is involved; advancing virtual time
 * by the configured idle timeout deterministically produces a heartbeat, and
 * closing the channel releases every queued task/buffer.
 */
@Tag("virtualtime")
class IdleEventVirtualTimeTest {

    private static final int IDLE_TIMEOUT_SECONDS = 2;

    private EmbeddedChannel newChannel() {
        final var messageFactory = ContractMessageFactory.server();
        return new EmbeddedChannel(
            new IdleStateHandler(0, 0, IDLE_TIMEOUT_SECONDS),
            new IdleEventHandler<IsoMessage>(messageFactory));
    }

    @Test
    void heartbeatIsSentWhenVirtualIdleTimeoutElapses() {
        final var channel = newChannel();
        try {
            // No heartbeat before the timeout.
            channel.advanceTimeBy(IDLE_TIMEOUT_SECONDS - 1L, TimeUnit.SECONDS);
            channel.runPendingTasks();
            assertThat(channel.<IsoMessage>readOutbound()).isNull();

            // Exactly at the timeout the IdleEventHandler writes a heartbeat (0x800).
            channel.advanceTimeBy(1, TimeUnit.SECONDS);
            channel.runPendingTasks();
            final var heartbeat = channel.<IsoMessage>readOutbound();
            assertThat(heartbeat).isNotNull();
            assertThat(heartbeat.getType()).isEqualTo(0x800);

            // Outbound queue is fully drained.
            assertThat(channel.<Object>readOutbound()).isNull();
        } finally {
            finishAndDrain(channel);
        }
    }

    @Test
    void twentyIdleIntervalsFireTwentyHeartbeatsWithoutExtraTasks() {
        final var channel = newChannel();
        try {
            for (var i = 0; i < 20; i++) {
                channel.advanceTimeBy(IDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                channel.runPendingTasks();
                final var heartbeat = channel.<IsoMessage>readOutbound();
                assertThat(heartbeat).as("heartbeat #%d", i + 1).isNotNull();
                assertThat(heartbeat.getType()).isEqualTo(0x800);
                // No extra queued message beyond the current heartbeat.
                assertThat((Object) channel.readOutbound()).isNull();
            }
        } finally {
            finishAndDrain(channel);
        }
    }

    private void finishAndDrain(final EmbeddedChannel channel) {
        channel.finishAndReleaseAll();
        assertThat(channel.isOpen()).isFalse();
    }
}

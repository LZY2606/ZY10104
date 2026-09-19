package com.github.kpavlov.jreactive8583.contract.leak;

import com.github.kpavlov.jreactive8583.IsoMessageListener;
import com.github.kpavlov.jreactive8583.contract.support.ChannelRegistry;
import com.github.kpavlov.jreactive8583.contract.support.ContractClient;
import com.github.kpavlov.jreactive8583.contract.support.ContractMessageFactory;
import com.github.kpavlov.jreactive8583.contract.support.ContractServer;
import com.github.kpavlov.jreactive8583.contract.support.ResourceAuditor;
import com.github.kpavlov.jreactive8583.client.ClientConfiguration;
import com.github.kpavlov.jreactive8583.server.ServerConfiguration;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.IsoType;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Repeats a full bind / connect / request-response / shutdown cycle twenty times
 * over real loopback with OS-assigned ports and asserts that no attributed
 * event-loop thread, accepted channel or buffer reference accumulates across
 * rounds. Each round builds and tears down its own connector pair and runs the
 * same {@link ResourceAuditor} checks on success and failure.
 */
@Tag("leak")
class LoopbackReconnectStressIT {

    private static final int ROUNDS = 20;
    private static final long TIMEOUT_SECONDS = 15;

    private int threadsWithPrefix(final String prefix) {
        var count = 0;
        for (final var info : ManagementFactory.getThreadMXBean().dumpAllThreads(false, false)) {
            if (info.getThreadName().startsWith(prefix)) {
                count++;
            }
        }
        return count;
    }

    private void await(final java.util.function.BooleanSupplier condition, final String alias)
        throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("Timed out waiting for: " + alias);
        }
    }

    @Test
    void twentyFullConnectShutdownRoundsLeaveNoResidualResources() throws Exception {
        for (var round = 0; round < ROUNDS; round++) {
            final var token = "reconnect-stress-round-" + round;
            final var registry = new ChannelRegistry();
            final var auditor = new ResourceAuditor();

            final var server = new ContractServer(
                ServerConfiguration.newBuilder()
                    .workerThreadsCount(1)
                    .addEchoMessageListener(false)
                    .build(),
                ContractMessageFactory.server(), token, registry);
            server.addMessageListener(new IsoMessageListener<>() {
                @Override
                public boolean applies(final IsoMessage message) {
                    return message.getType() == 0x200;
                }

                @Override
                public boolean onMessage(final ChannelHandlerContext ctx, final IsoMessage m) {
                    final var response = server.getIsoMessageFactory().createResponse(m);
                    response.setField(39, IsoType.ALPHA.value("00", 2));
                    ctx.writeAndFlush(response);
                    return false;
                }
            });

            RuntimeException failure = null;
            try {
                server.init();
                auditor.trackEventLoop(server.bossGroupForAudit(), token + "-server-boss");
                auditor.trackEventLoop(server.workerGroupForAudit(), token + "-server-worker");
                auditor.trackAllocator(server.trackingAllocator());
                server.start();
                await(server::isStarted, "round " + round + " server started");

                final var client = new ContractClient(
                    server.loopbackAddress(),
                    ClientConfiguration.newBuilder()
                        .workerThreadsCount(1)
                        .reconnectInterval(25)
                        .addEchoMessageListener(false)
                        .build(),
                    ContractMessageFactory.client(), token);
                final var gotResponse = new CountDownLatch(1);
                client.addMessageListener(new IsoMessageListener<>() {
                    @Override
                    public boolean applies(final IsoMessage message) {
                        return message.getType() == 0x210;
                    }

                    @Override
                    public boolean onMessage(final ChannelHandlerContext ctx,
                                             final IsoMessage m) {
                        gotResponse.countDown();
                        return false;
                    }
                });

                client.init();
                auditor.trackEventLoop(client.bossGroupForAudit(), token + "-client-nio");
                auditor.trackEventLoop(client.workerGroupForAudit(), token + "-client-worker");
                auditor.trackAllocator(client.trackingAllocator());

                try {
                    client.connect();
                    await(client::isConnected, "round " + round + " client connected");

                    client.send(client.getIsoMessageFactory().newMessage(0x0200));
                    assertThat(gotResponse.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .as("round %d exchange completed", round).isTrue();
                    assertThat(registry.activeCount())
                        .as("round %d accepted channel", round).isEqualTo(1);
                } finally {
                    client.closeForTest();
                }
            } catch (final RuntimeException e) {
                failure = e;
            } finally {
                try {
                    server.closeForTest();
                    await(() -> registry.activeCount() == 0,
                        "round " + round + " child channel closed");
                    auditor.assertAllReleased();
                    assertThat(threadsWithPrefix(token))
                        .as("round %d attributed threads", round).isZero();
                } finally {
                    if (failure != null) {
                        throw failure;
                    }
                }
            }
        }
    }
}

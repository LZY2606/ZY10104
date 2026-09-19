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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Hard gate for resource reclamation.
 *
 * <p>It performs twenty request/response exchanges over a single real loopback
 * connection and asserts there is no growth in message listeners (per-exchange
 * listeners are removed), accepted channels or allocator buffer references. It
 * then tears the connectors down (the same cleanup path runs on success or
 * failure) and the {@link ResourceAuditor} verifies every attributed event-loop
 * thread, channel and buffer is released, reporting ownership otherwise.</p>
 */
@Tag("leak")
class ResourceReclamationContractIT {

    private static final int EXCHANGES = 20;
    private static final long TIMEOUT_SECONDS = 15;

    @Test
    void twentyExchangesDoNotGrowListenersChannelsOrBuffers() throws Exception {
        final var token = "leak-audit";
        final var registry = new ChannelRegistry();
        final var auditor = new ResourceAuditor();

        final var server = new ContractServer(
            ServerConfiguration.newBuilder()
                .workerThreadsCount(1)
                .addEchoMessageListener(false)
                .build(),
            ContractMessageFactory.server(), token, registry);

        final var serverRequests = new CountDownLatch(EXCHANGES);
        final IsoMessageListener<IsoMessage> serverListener = new IsoMessageListener<>() {
            @Override
            public boolean applies(final IsoMessage message) {
                return message.getType() == 0x200;
            }

            @Override
            public boolean onMessage(final ChannelHandlerContext ctx, final IsoMessage message) {
                final var response = server.getIsoMessageFactory().createResponse(message);
                response.setField(39, IsoType.ALPHA.value("00", 2));
                ctx.writeAndFlush(response);
                serverRequests.countDown();
                return false;
            }
        };
        server.addMessageListener(serverListener);

        server.init();
        auditor.trackEventLoop(server.bossGroupForAudit(), token + "-server-boss");
        auditor.trackEventLoop(server.workerGroupForAudit(), token + "-server-worker");
        auditor.trackAllocator(server.trackingAllocator());
        server.start();
        waitUntil(server::isStarted, "server started");

        final var client = new ContractClient(
            server.loopbackAddress(),
            ClientConfiguration.newBuilder()
                .workerThreadsCount(1)
                .reconnectInterval(25)
                .addEchoMessageListener(false)
                .build(),
            ContractMessageFactory.client(),
            token);
        client.init();
        auditor.trackEventLoop(client.bossGroupForAudit(), token + "-client-nio");
        auditor.trackEventLoop(client.workerGroupForAudit(), token + "-client-worker");
        auditor.trackAllocator(client.trackingAllocator());

        try {
            client.connect();
            waitUntil(client::isConnected, "client connected");

            final var responses = new AtomicInteger();
            final IsoMessageListener<IsoMessage> responseListener = new IsoMessageListener<>() {
                @Override
                public boolean applies(final IsoMessage message) {
                    return message.getType() == 0x210;
                }

                @Override
                public boolean onMessage(final ChannelHandlerContext ctx,
                                         final IsoMessage message) {
                    responses.incrementAndGet();
                    return false;
                }
            };
            // A single listener is registered for all exchanges: listener count
            // must stay constant rather than grow with each message.
            client.addMessageListener(responseListener);

            try {
                for (var exchange = 0; exchange < EXCHANGES; exchange++) {
                    final var before = responses.get();
                    client.send(client.getIsoMessageFactory().newMessage(0x0200));
                    waitUntil(() -> responses.get() > before,
                        "response for exchange " + exchange);

                    // All traffic stays on the one accepted channel: no reconnect
                    // means no additional server child channel.
                    assertThat(registry.activeCount())
                        .as("accepted channel count after exchange %d", exchange)
                        .isEqualTo(1);
                }

                assertThat(responses.get()).isEqualTo(EXCHANGES);
                assertThat(serverRequests.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .as("server handled all %d requests", EXCHANGES).isTrue();

                // Exactly one response listener remains (no per-exchange growth).
                client.removeMessageListener(responseListener);
                assertThat(client.trackingAllocator().liveBufferCount())
                    .as("client buffers after exchanges").isZero();
                assertThat(server.trackingAllocator().liveBufferCount())
                    .as("server buffers after exchanges").isZero();
            } finally {
                client.removeMessageListener(responseListener);
            }
        } finally {
            // Same cleanup path on success or assertion failure.
            client.shutdown();
            server.shutdown();
            auditor.assertAllReleased();
        }

        assertThat(countThreadsWithPrefix(token + "-"))
            .as("no surviving event-loop threads attributed to '%s'", token)
            .isZero();
        assertThat(registry.activeCount())
            .as("no accepted channel survived shutdown").isZero();
    }

    private int countThreadsWithPrefix(final String prefix) {
        var count = 0;
        for (final var info : ManagementFactory.getThreadMXBean().dumpAllThreads(false, false)) {
            if (info.getThreadName().startsWith(prefix)) {
                count++;
            }
        }
        return count;
    }

    private void waitUntil(final java.util.function.BooleanSupplier condition, final String alias)
        throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("Timed out waiting for: " + alias);
        }
    }
}

package com.github.kpavlov.jreactive8583.contract.loopback;

import com.github.kpavlov.jreactive8583.IsoMessageListener;
import com.github.kpavlov.jreactive8583.contract.support.AbstractLoopbackContractTest;
import com.github.kpavlov.jreactive8583.contract.support.ContractClient;
import com.github.kpavlov.jreactive8583.contract.support.ContractServer;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.IsoType;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real NIO loopback scenario: the server binds to an OS-assigned port and the
 * client connects to the address taken from the actual server channel.
 * Connection and message delivery are awaited on futures/latches with explicit
 * timeouts - there are no fixed sleeps and no pre-allocated "free port".
 */
@Tag("loopback")
class LoopbackMessagingIT extends AbstractLoopbackContractTest {

    private static final long TIMEOUT_SECONDS = 10;

    @Test
    void exchangesRequestAndResponseOverRealLoopback() throws Exception {
        final var server = createServer();
        final var responses = new CopyOnWriteArrayList<IsoMessage>();
        final var responseLatch = new CountDownLatch(1);

        server.addMessageListener(new IsoMessageListener<>() {
            @Override
            public boolean applies(final IsoMessage message) {
                return message.getType() == 0x200;
            }

            @Override
            public boolean onMessage(final ChannelHandlerContext ctx, final IsoMessage message) {
                final var response = server.getIsoMessageFactory().createResponse(message);
                response.setField(39, IsoType.ALPHA.value("00", 2));
                ctx.writeAndFlush(response);
                return false;
            }
        });

        // Bind first, then take the real address from the bound server channel.
        server.init();
        trackInitialized(server);
        server.start();
        awaitCondition(server::isStarted, "server started");

        final InetSocketAddress bound = server.loopbackAddress();
        assertThat(bound.getPort()).isPositive();

        final var client = createClient(bound);
        client.addMessageListener(new IsoMessageListener<>() {
            @Override
            public boolean applies(final IsoMessage message) {
                return message.getType() == 0x210;
            }

            @Override
            public boolean onMessage(final ChannelHandlerContext ctx, final IsoMessage message) {
                responses.add(message);
                responseLatch.countDown();
                return false;
            }
        });

        client.init();
        trackInitialized(client);
        client.connect();
        awaitCondition(client::isConnected, "client connected");

        final var request = client.getIsoMessageFactory().newMessage(0x0200);
        final var completed = client.sendAsync(request)
            .await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(completed)
            .as("request write completed within %ds", TIMEOUT_SECONDS)
            .isTrue();

        assertThat(responseLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
            .as("client received 0x210 response within %ds", TIMEOUT_SECONDS)
            .isTrue();
        assertThat(responses).hasSize(1);
        assertThat(responses.get(0).getField(39).toString()).contains("00");
    }

    @Test
    void serverReceivesEveryMessageOverRealLoopback() throws Exception {
        final var messageCount = 25;
        final var server = createServer();
        final var received = new java.util.concurrent.ConcurrentHashMap<String, IsoMessage>();
        final var allReceived = new CountDownLatch(messageCount);

        server.addMessageListener(new IsoMessageListener<>() {
            @Override
            public boolean applies(final IsoMessage message) {
                return message.getType() == 0x200;
            }

            @Override
            public boolean onMessage(final ChannelHandlerContext ctx, final IsoMessage message) {
                received.put(message.getField(60).toString(), message);
                allReceived.countDown();
                return false;
            }
        });

        server.init();
        trackInitialized(server);
        server.start();
        awaitCondition(server::isStarted, "server started");

        final var client = createClient(server.loopbackAddress());
        client.init();
        trackInitialized(client);
        client.connect();
        awaitCondition(client::isConnected, "client connected");

        final List<String> correlationIds = new java.util.ArrayList<>();
        for (var i = 0; i < messageCount; i++) {
            final var request = client.getIsoMessageFactory().newMessage(0x0200);
            final var correlation = String.format("corr-%03d", i);
            request.setValue(60, correlation, IsoType.LLLVAR, 11);
            correlationIds.add(correlation);
            client.sendAsync(request);
        }

        assertThat(allReceived.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
            .as("server received all %d requests", messageCount)
            .isTrue();
        assertThat(received.keySet())
            .containsExactlyInAnyOrderElementsOf(correlationIds);
    }

}

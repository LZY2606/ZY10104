package com.github.kpavlov.jreactive8583.contract;

import com.github.kpavlov.jreactive8583.IsoMessageListener;
import com.github.kpavlov.jreactive8583.client.ClientConfiguration;
import com.github.kpavlov.jreactive8583.client.Iso8583Client;
import com.github.kpavlov.jreactive8583.iso.MessageOrigin;
import com.github.kpavlov.jreactive8583.netty.pipeline.CompositeIsoMessageHandler;
import com.github.kpavlov.jreactive8583.netty.pipeline.EchoMessageListener;
import com.github.kpavlov.jreactive8583.server.ServerConfiguration;
import com.solab.iso8583.IsoMessage;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Real loopback integration tests. Servers bind to an ephemeral port and the
 * address is read back from the actual channel; all waits are futures or
 * latches with explicit timeouts - never fixed sleeps. Every test releases
 * event loops, channels and allocator references, also on failure.
 */
class LoopbackContractIT {

    private ResourceAudit audit;
    private com.github.kpavlov.jreactive8583.iso.J8583MessageFactory<IsoMessage> serverMessages;
    private com.github.kpavlov.jreactive8583.iso.J8583MessageFactory<IsoMessage> clientMessages;

    @BeforeEach
    void setUp() throws Exception {
        audit = new ResourceAudit();
        serverMessages = TestMessages.messageFactory(MessageOrigin.ACQUIRER);
        clientMessages = TestMessages.messageFactory(MessageOrigin.ACQUIRER);
    }

    @AfterEach
    void tearDown() {
        audit.closeAll();
        audit.assertAllReleased();
    }

    @Test
    void echoRoundTripOverDynamicallyBoundPort() throws Exception {
        final LoopbackServer server = startServer(0);
        final InetSocketAddress address = server.address();
        assertThat(address.getPort()).as("port read from the bound channel").isNotZero();

        final Iso8583Client<IsoMessage> client = newClient(address);
        final var echoResponse = new CountDownLatch(1);
        client.addMessageListener(latchOn(TestMessages.ECHO_RESPONSE_MTI, echoResponse));
        client.init();
        try {
            client.connect();
            await().atMost(TestMessages.TIMEOUT).until(client::isConnected);

            client.send(TestMessages.newEchoRequest(clientMessages));
            assertThat(echoResponse.await(TestMessages.TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("echo response received").isTrue();
        } finally {
            client.disconnectAsync();
            client.shutdown();
        }
    }

    @Test
    void clientReconnectsToServerRestartedOnSamePort() throws Exception {
        LoopbackServer server = startServer(0);
        final int port = server.address().getPort();

        final Iso8583Client<IsoMessage> client = newClient(server.address());
        final var firstResponse = new CountDownLatch(1);
        final var firstListener = latchOn(TestMessages.ECHO_RESPONSE_MTI, firstResponse);
        client.addMessageListener(firstListener);
        client.init();
        try {
            client.connect();
            client.send(TestMessages.newEchoRequest(clientMessages));
            assertThat(firstResponse.await(TestMessages.TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("first echo response").isTrue();
            client.removeMessageListener(firstListener);

            server.close();
            await().atMost(TestMessages.TIMEOUT).until(() -> !client.isConnected());

            server = startServer(port);
            await().atMost(TestMessages.TIMEOUT).until(client::isConnected);

            final var secondResponse = new CountDownLatch(1);
            client.addMessageListener(latchOn(TestMessages.ECHO_RESPONSE_MTI, secondResponse));
            client.send(TestMessages.newEchoRequest(clientMessages));
            assertThat(secondResponse.await(TestMessages.TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("echo response after reconnect").isTrue();
        } finally {
            client.disconnectAsync();
            client.shutdown();
            server.close();
        }
    }

    @Test
    void connectionRefusedFailsTheConnectFuture() throws Exception {
        final Iso8583Client<IsoMessage> client =
            newClient(new InetSocketAddress("127.0.0.1", unusedLoopbackPort()));
        client.init();
        try {
            final ChannelFuture connectFuture = client.connectAsync();
            assertThat(connectFuture.awaitUninterruptibly(TestMessages.TIMEOUT.toMillis()))
                .as("connect future completed").isTrue();
            assertThat(connectFuture.isSuccess()).isFalse();
            assertThat(connectFuture.cause()).isInstanceOf(ConnectException.class);
        } finally {
            client.disconnectAsync();
            client.shutdown();
        }
    }

    private LoopbackServer startServer(final int port) {
        final var configuration = ServerConfiguration.newBuilder().workerThreadsCount(1).build();
        final var handler = new CompositeIsoMessageHandler<IsoMessage>();
        handler.addListener(new EchoMessageListener<>(serverMessages));
        return LoopbackServer.start(audit, configuration, handler, serverMessages, port);
    }

    private Iso8583Client<IsoMessage> newClient(final InetSocketAddress address) {
        final var configuration = ClientConfiguration.newBuilder()
            .reconnectInterval(200)
            .workerThreadsCount(1)
            .build();
        return new Iso8583Client<>(address, configuration, clientMessages);
    }

    static IsoMessageListener<IsoMessage> latchOn(final int mti, final CountDownLatch latch) {
        return new IsoMessageListener<>() {
            @Override
            public boolean applies(final IsoMessage isoMessage) {
                return isoMessage.getType() == mti;
            }

            @Override
            public boolean onMessage(final ChannelHandlerContext ctx, final IsoMessage isoMessage) {
                latch.countDown();
                return false;
            }
        };
    }

    /** A port that is refusing connections: bound ephemeral, then immediately released. */
    private static int unusedLoopbackPort() throws IOException {
        try (var socket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            return socket.getLocalPort();
        }
    }
}

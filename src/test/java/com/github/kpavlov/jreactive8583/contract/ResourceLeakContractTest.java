package com.github.kpavlov.jreactive8583.contract;

import com.github.kpavlov.jreactive8583.IsoMessageListener;
import com.github.kpavlov.jreactive8583.client.ClientConfiguration;
import com.github.kpavlov.jreactive8583.client.Iso8583Client;
import com.github.kpavlov.jreactive8583.iso.MessageOrigin;
import com.github.kpavlov.jreactive8583.netty.codec.Iso8583Decoder;
import com.github.kpavlov.jreactive8583.netty.pipeline.CompositeIsoMessageHandler;
import com.github.kpavlov.jreactive8583.netty.pipeline.EchoMessageListener;
import com.github.kpavlov.jreactive8583.server.ServerConfiguration;
import com.solab.iso8583.IsoMessage;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs repeated connect/echo/fault rounds and asserts after every round that
 * event loops, scheduled reconnects, channels and allocator references are
 * released, and that no listeners or threads accumulate across rounds.
 */
class ResourceLeakContractTest {

    private static final int ROUNDS = 20;

    @Test
    void repeatedRoundsDoNotLeakThreadsListenersOrAllocatorMemory() throws Exception {
        final var audit = new ResourceAudit();
        final var serverMessages = TestMessages.messageFactory(MessageOrigin.ACQUIRER);
        final var clientMessages = TestMessages.messageFactory(MessageOrigin.ACQUIRER);
        final var sharedServerHandler = new CompositeIsoMessageHandler<IsoMessage>();
        sharedServerHandler.addListener(new EchoMessageListener<>(serverMessages));
        final int baselineListeners = listenerCount(sharedServerHandler);

        for (int round = 0; round < ROUNDS; round++) {
            try {
                runFaultRound(round, clientMessages);
                runLoopbackRound(audit, serverMessages, clientMessages, sharedServerHandler);
            } finally {
                audit.closeAll();
            }
            audit.assertAllReleased();
        }

        assertThat(listenerCount(sharedServerHandler))
            .as("no listener growth after %s rounds", ROUNDS)
            .isEqualTo(baselineListeners);
    }

    /** Deterministic fault-injection round on an EmbeddedChannel (no OS resources). */
    private void runFaultRound(final int round, final com.github.kpavlov.jreactive8583.iso.J8583MessageFactory<IsoMessage> messages) {
        final byte[] frame = TestMessages.encodeFrame(TestMessages.newEchoRequest(messages));
        final var script = new FaultScript(round);
        final var channel = new EmbeddedChannel(
            new LengthFieldBasedFrameDecoder(8192, 0, 2, 0, 2),
            new Iso8583Decoder(messages));
        try {
            for (final byte[] fragment : script.fragment(frame)) {
                channel.writeInbound(Unpooled.wrappedBuffer(fragment));
            }
            final IsoMessage decoded = channel.readInbound();
            assertThat(decoded.getType()).as("round %s", round).isEqualTo(TestMessages.ECHO_REQUEST_MTI);
        } finally {
            assertThat(channel.finishAndReleaseAll()).as("round %s", round).isFalse();
        }
    }

    /** Real loopback round on a dynamically bound port with latch-based waits. */
    private void runLoopbackRound(
        final ResourceAudit audit,
        final com.github.kpavlov.jreactive8583.iso.J8583MessageFactory<IsoMessage> serverMessages,
        final com.github.kpavlov.jreactive8583.iso.J8583MessageFactory<IsoMessage> clientMessages,
        final CompositeIsoMessageHandler<IsoMessage> sharedServerHandler
    ) throws Exception {
        final IsoMessageListener<IsoMessage> roundListener = new IsoMessageListener<>() {
            @Override
            public boolean applies(final IsoMessage isoMessage) {
                return false;
            }

            @Override
            public boolean onMessage(final ChannelHandlerContext ctx, final IsoMessage isoMessage) {
                return true;
            }
        };
        sharedServerHandler.addListener(roundListener);
        final var serverConfig = ServerConfiguration.newBuilder().workerThreadsCount(1).build();
        final LoopbackServer server =
            LoopbackServer.start(audit, serverConfig, sharedServerHandler, serverMessages);
        Iso8583Client<IsoMessage> client = null;
        try {
            final var clientConfig = ClientConfiguration.newBuilder()
                .reconnectInterval(100)
                .workerThreadsCount(1)
                .build();
            client = new Iso8583Client<>(server.address(), clientConfig, clientMessages);
            final var echoResponse = new CountDownLatch(1);
            client.addMessageListener(LoopbackContractIT.latchOn(TestMessages.ECHO_RESPONSE_MTI, echoResponse));
            client.init();
            client.connect();
            await().atMost(TestMessages.TIMEOUT).until(client::isConnected);
            client.send(TestMessages.newEchoRequest(clientMessages));
            assertThat(echoResponse.await(TestMessages.TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("echo response").isTrue();
        } finally {
            if (client != null) {
                client.disconnectAsync();
                client.shutdown();
            }
            sharedServerHandler.removeListener(roundListener);
            server.close();
        }
    }

    private static int listenerCount(final CompositeIsoMessageHandler<?> handler) throws Exception {
        final Field field = CompositeIsoMessageHandler.class.getDeclaredField("messageListeners");
        field.setAccessible(true);
        return ((List<?>) field.get(handler)).size();
    }
}

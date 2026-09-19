package com.github.kpavlov.jreactive8583.contract;

import com.solab.iso8583.IsoMessage;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import com.github.kpavlov.jreactive8583.iso.MessageOrigin;
import com.github.kpavlov.jreactive8583.netty.codec.Iso8583Decoder;
import com.github.kpavlov.jreactive8583.netty.codec.Iso8583Encoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deterministic network-fault fixtures on {@link EmbeddedChannel}: fragmented
 * length prefixes, coalesced frames, half-frame close and write failures.
 * Every scenario is reproducible from its seed.
 */
class FaultInjectionContractTest {

    private com.github.kpavlov.jreactive8583.iso.J8583MessageFactory<IsoMessage> messageFactory;

    @BeforeEach
    void setUp() throws Exception {
        messageFactory = TestMessages.messageFactory(MessageOrigin.ACQUIRER);
    }

    @Test
    void fragmentedLengthPrefixFramesAreReassembledDeterministically() {
        for (long seed = 1; seed <= 20; seed++) {
            final byte[] frame = TestMessages.encodeFrame(TestMessages.newEchoRequest(messageFactory));
            final var script = new FaultScript(seed);
            final var channel = newDecoderChannel();
            try {
                for (final byte[] fragment : script.fragment(frame)) {
                    channel.writeInbound(Unpooled.wrappedBuffer(fragment));
                    script.record("writeInbound bytes=" + fragment.length);
                }
                final IsoMessage decoded = channel.readInbound();
                assertThat(decoded.getType()).as("seed %s", seed).isEqualTo(TestMessages.ECHO_REQUEST_MTI);
                assertThat((Object) channel.readInbound()).as("seed %s", seed).isNull();
            } finally {
                assertThat(channel.finishAndReleaseAll()).as("seed %s", seed).isFalse();
            }
        }
    }

    @Test
    void coalescedFramesAreSplitIntoIndividualMessages() {
        final byte[] first = TestMessages.encodeFrame(TestMessages.newEchoRequest(messageFactory));
        final byte[] second = TestMessages.encodeFrame(TestMessages.newEchoRequest(messageFactory));
        final var script = new FaultScript(7L);
        script.record("coalesce frames=2");
        final var channel = newDecoderChannel();
        try {
            channel.writeInbound(Unpooled.wrappedBuffer(first, second));
            assertThat(((IsoMessage) channel.readInbound()).getType()).isEqualTo(TestMessages.ECHO_REQUEST_MTI);
            assertThat(((IsoMessage) channel.readInbound()).getType()).isEqualTo(TestMessages.ECHO_REQUEST_MTI);
            assertThat((Object) channel.readInbound()).isNull();
        } finally {
            assertThat(channel.finishAndReleaseAll()).isFalse();
        }
    }

    @Test
    void halfFrameThenCloseProducesNoMessageAndNoException() {
        final byte[] frame = TestMessages.encodeFrame(TestMessages.newEchoRequest(messageFactory));
        final var script = new FaultScript(11L);
        final var channel = newDecoderChannel();
        channel.writeInbound(Unpooled.wrappedBuffer(script.halfFrame(frame)));
        channel.close();
        assertThat((Object) channel.readInbound()).as("no message from a half frame").isNull();
        assertThat(channel.isOpen()).isFalse();
        channel.finishAndReleaseAll();
    }

    @Test
    void writeFailureIsPropagatedToTheWriteFuture() {
        final var script = new FaultScript(13L);
        final var failure = new IOException("simulated write failure seed=13");
        final var channel = new EmbeddedChannel(
            new Iso8583Encoder(2, false),
            new ChannelOutboundHandlerAdapter() {
                @Override
                public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
                    script.record("writeFailed " + msg.getClass().getSimpleName());
                    promise.setFailure(failure);
                }
            });
        try {
            final ChannelFuture future = channel.writeAndFlush(TestMessages.newEchoRequest(messageFactory));
            assertThat(future.isDone()).isTrue();
            assertThat(future.isSuccess()).isFalse();
            assertThat(future.cause()).isSameAs(failure);
            assertThat(script.timeline()).contains("writeFailed IsoMessage");
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void sameSeedProducesIdenticalEventTimeline() {
        final List<String> first = runFaultScenario(42L);
        final List<String> second = runFaultScenario(42L);
        assertThat(first).isNotEmpty();
        assertThat(second).isEqualTo(first);
    }

    private List<String> runFaultScenario(final long seed) {
        final var script = new FaultScript(seed);
        final byte[] frame = TestMessages.encodeFrame(TestMessages.newEchoRequest(messageFactory));
        final var channel = newDecoderChannel();
        try {
            for (final byte[] fragment : script.fragment(frame)) {
                channel.writeInbound(Unpooled.wrappedBuffer(fragment));
                script.record("writeInbound bytes=" + fragment.length);
            }
            final IsoMessage decoded = channel.readInbound();
            script.record("decoded mti=" + Integer.toHexString(decoded.getType()));
        } finally {
            channel.finishAndReleaseAll();
        }
        return script.timeline();
    }

    private EmbeddedChannel newDecoderChannel() {
        return new EmbeddedChannel(
            new LengthFieldBasedFrameDecoder(8192, 0, 2, 0, 2),
            new Iso8583Decoder(messageFactory));
    }
}

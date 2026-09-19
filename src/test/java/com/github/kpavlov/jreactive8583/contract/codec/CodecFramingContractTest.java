package com.github.kpavlov.jreactive8583.contract.codec;

import com.github.kpavlov.jreactive8583.contract.support.ContractMessageFactory;
import com.github.kpavlov.jreactive8583.contract.support.FaultScript;
import com.github.kpavlov.jreactive8583.contract.support.TrackingByteBufAllocator;
import com.github.kpavlov.jreactive8583.iso.MessageFactory;
import com.github.kpavlov.jreactive8583.netty.codec.Iso8583Decoder;
import com.github.kpavlov.jreactive8583.netty.codec.Iso8583Encoder;
import com.github.kpavlov.jreactive8583.netty.codec.StringLengthFieldBasedFrameDecoder;
import com.solab.iso8583.IsoMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deterministically exercises the length-prefix codec against byte-stream faults
 * using an EmbeddedChannel: prefix fragmentation, sticky packets, half-frame
 * followed by close, and write failures. No socket, thread or wall-clock timing
 * is involved; the same seed yields the same delivery timeline.
 */
@Tag("codec")
class CodecFramingContractTest {

    private final TrackingByteBufAllocator allocator =
        new TrackingByteBufAllocator("codec-framing");

    private static final class MessageCollector extends ChannelInboundHandlerAdapter {
        private final List<IsoMessage> messages = new ArrayList<>();

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            messages.add((IsoMessage) msg);
        }
    }

    private EmbeddedChannel binaryChannel(final MessageCollector collector) {
        final var channel = new EmbeddedChannel(
            new LengthFieldBasedFrameDecoder(8192, 0, 2, 0, 2),
            new Iso8583Decoder(ContractMessageFactory.server()),
            new Iso8583Encoder(2, false),
            collector);
        channel.config().setAllocator(allocator);
        return channel;
    }

    private EmbeddedChannel asciiChannel() {
        final var channel = new EmbeddedChannel(
            new StringLengthFieldBasedFrameDecoder(8192, 0, 4, 0, 4),
            new Iso8583Decoder(ContractMessageFactory.server()),
            new Iso8583Encoder(4, true));
        channel.config().setAllocator(allocator);
        return channel;
    }

    private byte[] encodeBinaryFrame(final int type) {
        final var factory = ContractMessageFactory.server();
        final var message = factory.newMessage(type);
        final var channel = binaryChannel(new MessageCollector());
        try {
            assertThat(channel.writeOutbound(message)).isTrue();
            return readAllOutbound(channel);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private byte[] readAllOutbound(final EmbeddedChannel channel) {
        final var chunks = new java.io.ByteArrayOutputStream();
        ByteBuf chunk;
        while ((chunk = channel.readOutbound()) != null) {
            final var bytes = new byte[chunk.readableBytes()];
            chunk.readBytes(bytes);
            chunks.write(bytes, 0, bytes.length);
            chunk.release();
        }
        return chunks.toByteArray();
    }

    @Test
    void reassemblesLengthPrefixSplitAcrossChunks() {
        final var frame = encodeBinaryFrame(0x0200);
        final var script = FaultScript.forFrames(123456789L, frame);

        final var collector = new MessageCollector();
        final var channel = binaryChannel(collector);
        try {
            for (final var event : script.events()) {
                channel.writeInbound(Unpooled.wrappedBuffer(event.data()));
            }
            assertThat(collector.messages).hasSize(1);
            assertThat(collector.messages.get(0).getType()).isEqualTo(0x0200);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void splitsTwoStickyFramesIntoTwoMessages() {
        final var first = encodeBinaryFrame(0x0200);
        final var second = encodeBinaryFrame(0x0200);
        final var script = FaultScript.stickyFrames(987654321L, first, second);

        final var collector = new MessageCollector();
        final var channel = binaryChannel(collector);
        try {
            for (final var event : script.events()) {
                channel.writeInbound(Unpooled.wrappedBuffer(event.data()));
            }
            assertThat(collector.messages).hasSize(2);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void halfFrameFollowedByCloseDoesNotEmitPartialMessage() {
        final var frame = encodeBinaryFrame(0x0200);
        final var script = FaultScript.halfFrameThenClose(55555L, frame);

        final var collector = new MessageCollector();
        final var channel = binaryChannel(collector);
        try {
            for (final var event : script.events()) {
                switch (event.type()) {
                    case DELIVER, HALF_FRAME ->
                        channel.writeInbound(Unpooled.wrappedBuffer(event.data()));
                    case CLOSE_AFTER -> channel.close();
                }
            }
            channel.runPendingTasks();
            // A truncated frame must never be decoded as a complete message.
            assertThat(collector.messages).isEmpty();
            assertThat(channel.isOpen()).isFalse();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void writeFailureIsSurfacedAndResourcesAreReleased() {
        final var channel = asciiChannel();
        final var factory = ContractMessageFactory.client();
        final var message = factory.newMessage(0x0800);
        try {
            channel.writeOutbound(message);
            final var outbound = (ByteBuf) channel.readOutbound();
            // ASCII 4-digit length prefix followed by the message bytes.
            final var prefixBytes = new byte[4];
            outbound.readBytes(prefixBytes);
            final var prefix = new String(prefixBytes, StandardCharsets.US_ASCII);
            assertThat(Integer.parseInt(prefix)).isPositive();
            outbound.release();

            // Force a downstream write failure through a failing outbound handler.
            final var failure = new IllegalStateException("forced downstream write failure");
            final var failing = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
                @Override
                public void write(final io.netty.channel.ChannelHandlerContext ctx,
                                  final Object msg, final ChannelPromise promise) {
                    promise.tryFailure(failure);
                }
            }, new Iso8583Encoder(4, true));
            failing.config().setAllocator(allocator);
            try {
                final var writeFuture = failing.writeOneOutbound(factory.newMessage(0x0800));
                assertThat(writeFuture.isDone()).isTrue();
                assertThat(writeFuture.isSuccess()).isFalse();
                assertThat(writeFuture.cause()).isSameAs(failure);
            } finally {
                failing.finishAndReleaseAll();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /**
     * Replays the same seed 20 times: the split timeline and decoded result must
     * be identical every time, guarding against nondeterministic fault injection.
     */
    @Test
    void sameSeedProducesSameTimelineAcrossTwentyRuns() {
        final var frame = encodeBinaryFrame(0x0200);
        String referenceTimeline = null;
        for (var round = 0; round < 20; round++) {
            final var script = FaultScript.forFrames(777L, frame);
            final var labels = new ArrayList<String>();
            final var collector = new MessageCollector();
            final var channel = binaryChannel(collector);
            try {
                for (final var event : script.events()) {
                    labels.add(event.label() + ":" + event.data().length);
                    channel.writeInbound(Unpooled.wrappedBuffer(event.data()));
                }
            } finally {
                channel.finishAndReleaseAll();
            }
            final var timeline = String.join(",", labels);
            if (referenceTimeline == null) {
                referenceTimeline = timeline;
            } else {
                assertThat(timeline).isEqualTo(referenceTimeline);
            }
            assertThat(collector.messages).hasSize(1);
        }
    }
}

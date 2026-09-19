package com.github.kpavlov.jreactive8583.contract;

import com.github.kpavlov.jreactive8583.iso.ISO8583Version;
import com.github.kpavlov.jreactive8583.iso.J8583MessageFactory;
import com.github.kpavlov.jreactive8583.iso.MessageClass;
import com.github.kpavlov.jreactive8583.iso.MessageFactory;
import com.github.kpavlov.jreactive8583.iso.MessageFunction;
import com.github.kpavlov.jreactive8583.iso.MessageOrigin;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.parse.ConfigParser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** Shared helpers for building ISO-8583 test messages and wire frames. */
final class TestMessages {

    static final Duration TIMEOUT = Duration.ofSeconds(15);

    static final int ECHO_REQUEST_MTI = 0x800;
    static final int ECHO_RESPONSE_MTI = 0x810;

    private TestMessages() {
    }

    static J8583MessageFactory<IsoMessage> messageFactory(final MessageOrigin origin) throws IOException {
        final var messageFactory = ConfigParser.createDefault();
        messageFactory.setCharacterEncoding(StandardCharsets.US_ASCII.name());
        messageFactory.setUseBinaryMessages(false);
        messageFactory.setAssignDate(true);
        return new J8583MessageFactory<>(messageFactory, ISO8583Version.V1987, origin);
    }

    static IsoMessage newEchoRequest(final MessageFactory<IsoMessage> messageFactory) {
        return messageFactory.newMessage(MessageClass.NETWORK_MANAGEMENT, MessageFunction.REQUEST);
    }

    /** Encodes a message as a frame: 2-byte binary length header followed by the message body. */
    static byte[] encodeFrame(final IsoMessage message) {
        final byte[] body = message.writeData();
        final ByteBuffer buffer = ByteBuffer.allocate(2 + body.length);
        buffer.putShort((short) body.length);
        buffer.put(body);
        return buffer.array();
    }
}

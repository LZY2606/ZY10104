package com.github.kpavlov.jreactive8583.contract.support;

import com.github.kpavlov.jreactive8583.iso.ISO8583Version;
import com.github.kpavlov.jreactive8583.iso.J8583MessageFactory;
import com.github.kpavlov.jreactive8583.iso.MessageOrigin;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.impl.SimpleTraceGenerator;
import com.solab.iso8583.parse.ConfigParser;

import java.nio.charset.StandardCharsets;

/**
 * Creates identical, self-contained message factories for contract tests so the
 * tests do not depend on Spring or the example configuration.
 */
public final class ContractMessageFactory {

    private ContractMessageFactory() {
    }

    public static J8583MessageFactory<IsoMessage> client() {
        return create(MessageOrigin.OTHER);
    }

    public static J8583MessageFactory<IsoMessage> server() {
        return create(MessageOrigin.ACQUIRER);
    }

    private static J8583MessageFactory<IsoMessage> create(final MessageOrigin origin) {
        final com.solab.iso8583.MessageFactory<IsoMessage> factory;
        try {
            factory = ConfigParser.createDefault();
        } catch (final java.io.IOException e) {
            throw new IllegalStateException("Failed to create default j8583 factory", e);
        }
        factory.setCharacterEncoding(StandardCharsets.US_ASCII.name());
        factory.setUseBinaryMessages(false);
        factory.setAssignDate(true);
        factory.setTraceNumberGenerator(new SimpleTraceGenerator(1));
        return new J8583MessageFactory<>(factory, ISO8583Version.V1987, origin);
    }
}

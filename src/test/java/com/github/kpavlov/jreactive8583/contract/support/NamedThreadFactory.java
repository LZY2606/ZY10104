package com.github.kpavlov.jreactive8583.contract.support;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Daemon thread factory that names every created thread with an explicit
 * ownership prefix. The resource auditor uses these names to attribute
 * surviving threads to a concrete fixture, instead of matching Netty's
 * generic "nioEventLoopGroup-N" counter.
 */
public final class NamedThreadFactory implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger sequence = new AtomicInteger();

    public NamedThreadFactory(final String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        final var thread = new Thread(runnable, prefix + "-" + sequence.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    }

    public String prefix() {
        return prefix;
    }
}

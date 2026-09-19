package com.github.kpavlov.jreactive8583.contract.support;

import com.github.kpavlov.jreactive8583.ConnectorConfigurer;
import com.github.kpavlov.jreactive8583.client.ClientConfiguration;
import com.github.kpavlov.jreactive8583.client.Iso8583Client;
import com.github.kpavlov.jreactive8583.iso.MessageFactory;
import com.solab.iso8583.IsoMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client fixture whose event-loop threads carry an attribution prefix and whose
 * connect attempts are counted. The prefix lets the resource auditor attribute
 * any surviving thread to this exact fixture instance.
 */
public final class ContractClient extends Iso8583Client<IsoMessage> {

    private final String ownerToken;
    private final TrackingByteBufAllocator allocator;
    private final AtomicInteger connectAttempts = new AtomicInteger();

    public ContractClient(
        final SocketAddress socketAddress,
        final ClientConfiguration configuration,
        final MessageFactory<IsoMessage> messageFactory,
        final String ownerToken
    ) {
        super(socketAddress, configuration, messageFactory);
        this.ownerToken = ownerToken;
        this.allocator = new TrackingByteBufAllocator(ownerToken + ":client");
        setConfigurer(new ConnectorConfigurer<ClientConfiguration, Bootstrap>() {
            @Override
            public void configureBootstrap(final Bootstrap bootstrap,
                                          final ClientConfiguration configuration) {
                bootstrap.option(ChannelOption.ALLOCATOR, allocator);
            }
        });
    }

    public String ownerToken() {
        return ownerToken;
    }

    public TrackingByteBufAllocator trackingAllocator() {
        return allocator;
    }

    public int connectAttemptCount() {
        return connectAttempts.get();
    }

    /** Boss group is the group the client schedules reconnects on. */
    public EventLoopGroup bossGroupForAudit() {
        return getBossEventLoopGroup();
    }

    public EventLoopGroup workerGroupForAudit() {
        return getWorkerEventLoopGroup();
    }

    /**
     * Closes the connection and cancels reconnect intent without shutting the
     * event loop groups down. The owning {@link ResourceAuditor} performs the
     * (zero-quiet) group shutdown, avoiding the default shutdown delay in tests.
     */
    public void closeForTest() {
        final var closeFuture = disconnectAsync();
        if (closeFuture != null) {
            closeFuture.awaitUninterruptibly();
        }
    }

    @Override
    public ChannelFuture connectAsync() {
        connectAttempts.incrementAndGet();
        return super.connectAsync();
    }

    @Override
    protected EventLoopGroup createBossEventLoopGroup() {
        return new NioEventLoopGroup(1, new NamedThreadFactory(ownerToken + "-client-nio"));
    }

    @Override
    protected EventLoopGroup createWorkerEventLoopGroup() {
        return new NioEventLoopGroup(1, new NamedThreadFactory(ownerToken + "-client-worker"));
    }
}

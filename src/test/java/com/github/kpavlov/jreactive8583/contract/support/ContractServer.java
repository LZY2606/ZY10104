package com.github.kpavlov.jreactive8583.contract.support;

import com.github.kpavlov.jreactive8583.ConnectorConfigurer;
import com.github.kpavlov.jreactive8583.iso.MessageFactory;
import com.github.kpavlov.jreactive8583.server.Iso8583Server;
import com.github.kpavlov.jreactive8583.server.ServerConfiguration;
import com.solab.iso8583.IsoMessage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;

/**
 * Server fixture that binds to an OS-assigned port, exposes the actually bound
 * address (so tests never guess a free port), attributes its event-loop threads,
 * and records accepted channels and allocator references.
 */
public final class ContractServer extends Iso8583Server<IsoMessage> {

    private final String ownerToken;
    private final TrackingByteBufAllocator allocator;
    private final ChannelRegistry childChannels;

    public ContractServer(
        final ServerConfiguration configuration,
        final MessageFactory<IsoMessage> messageFactory,
        final String ownerToken,
        final ChannelRegistry childChannels
    ) {
        // Port 0 = let the OS assign the port; read the real address after start().
        super(0, configuration, messageFactory);
        this.ownerToken = ownerToken;
        this.allocator = new TrackingByteBufAllocator(ownerToken + ":server");
        this.childChannels = childChannels;
        setConfigurer(new ConnectorConfigurer<>() {
            @Override
            public void configureBootstrap(final ServerBootstrap bootstrap,
                                          final ServerConfiguration configuration) {
                bootstrap.option(ChannelOption.ALLOCATOR, allocator);
                bootstrap.childOption(ChannelOption.ALLOCATOR, allocator);
            }

            @Override
            public void configurePipeline(final io.netty.channel.ChannelPipeline pipeline,
                                         final ServerConfiguration configuration) {
                pipeline.addLast("contractChildRegistry",
                    new ChannelRegistry.PerChannel(childChannels));
            }
        });
    }

    public String ownerToken() {
        return ownerToken;
    }

    public TrackingByteBufAllocator trackingAllocator() {
        return allocator;
    }

    public ChannelRegistry childChannelRegistry() {
        return childChannels;
    }

    public EventLoopGroup bossGroupForAudit() {
        return getBossEventLoopGroup();
    }

    public EventLoopGroup workerGroupForAudit() {
        return getWorkerEventLoopGroup();
    }

    /**
     * Closes the server (accepting) channel without shutting the event loop
     * groups down; the owning {@link ResourceAuditor} shuts them down with a
     * zero quiet period.
     */
    public void closeForTest() {
        stop();
    }

    /**
     * Returns the address the server channel is actually bound to.
     * Must be called after {@link #start()}.
     */
    public InetSocketAddress boundAddress() {
        final var channel = getChannel();
        final var address = channel == null ? null : channel.localAddress();
        if (!(address instanceof InetSocketAddress inet) || inet.getPort() == 0) {
            throw new IllegalStateException("Server is not bound yet: " + address);
        }
        return inet;
    }

    /**
     * Loopback address pointing at the actually bound port. The server binds a
     * wildcard address; clients must connect to the concrete loopback host rather
     * than the wildcard, which can be rejected/closed immediately.
     */
    public InetSocketAddress loopbackAddress() {
        return new InetSocketAddress("127.0.0.1", boundAddress().getPort());
    }

    @Override
    protected EventLoopGroup createBossEventLoopGroup() {
        return new NioEventLoopGroup(1, new NamedThreadFactory(ownerToken + "-server-boss"));
    }

    @Override
    protected EventLoopGroup createWorkerEventLoopGroup() {
        return new NioEventLoopGroup(1, new NamedThreadFactory(ownerToken + "-server-worker"));
    }
}

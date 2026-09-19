package com.github.kpavlov.jreactive8583.contract;

import com.github.kpavlov.jreactive8583.iso.MessageFactory;
import com.github.kpavlov.jreactive8583.netty.pipeline.CompositeIsoMessageHandler;
import com.github.kpavlov.jreactive8583.netty.pipeline.Iso8583ChannelInitializer;
import com.github.kpavlov.jreactive8583.server.ServerConfiguration;
import com.solab.iso8583.IsoMessage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

/**
 * Loopback ISO-8583 server bound to an ephemeral port. The actual address is
 * always read back from the bound channel - no "find a free port" probing.
 */
public final class LoopbackServer implements AutoCloseable {

    private final NioEventLoopGroup bossGroup;
    private final NioEventLoopGroup workerGroup;
    private final Channel serverChannel;

    private LoopbackServer(
        final NioEventLoopGroup bossGroup,
        final NioEventLoopGroup workerGroup,
        final Channel serverChannel
    ) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.serverChannel = serverChannel;
    }

    /** Binds to {@code 127.0.0.1:0} and returns once the channel is bound. */
    public static LoopbackServer start(
        final ResourceAudit audit,
        final ServerConfiguration configuration,
        final CompositeIsoMessageHandler<IsoMessage> messageHandler,
        final MessageFactory<IsoMessage> messageFactory
    ) {
        return start(audit, configuration, messageHandler, messageFactory, 0);
    }

    /** Binds to {@code 127.0.0.1:port} (0 = ephemeral) and returns once the channel is bound. */
    public static LoopbackServer start(
        final ResourceAudit audit,
        final ServerConfiguration configuration,
        final CompositeIsoMessageHandler<IsoMessage> messageHandler,
        final MessageFactory<IsoMessage> messageFactory,
        final int port
    ) {
        final NioEventLoopGroup boss = audit.track("server.bossEventLoopGroup", new NioEventLoopGroup(1));
        final NioEventLoopGroup worker =
            audit.track("server.workerEventLoopGroup", new NioEventLoopGroup(configuration.getWorkerThreadsCount()));
        final ServerBootstrap bootstrap = new ServerBootstrap()
            .group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(new Iso8583ChannelInitializer<Channel, ServerBootstrap, ServerConfiguration>(
                configuration, null, worker, messageFactory, messageHandler));
        final Channel channel = bootstrap.bind(new InetSocketAddress("127.0.0.1", port))
            .syncUninterruptibly()
            .channel();
        audit.track("server.channel", channel);
        return new LoopbackServer(boss, worker, channel);
    }

    /** The address the server channel is actually bound to. */
    public InetSocketAddress address() {
        return (InetSocketAddress) serverChannel.localAddress();
    }

    @Override
    public void close() {
        serverChannel.close().awaitUninterruptibly(TestMessages.TIMEOUT.toMillis());
        bossGroup.shutdownGracefully().awaitUninterruptibly(TestMessages.TIMEOUT.toMillis());
        workerGroup.shutdownGracefully().awaitUninterruptibly(TestMessages.TIMEOUT.toMillis());
    }
}

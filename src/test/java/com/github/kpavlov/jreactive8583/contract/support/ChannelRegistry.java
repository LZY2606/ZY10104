package com.github.kpavlov.jreactive8583.contract.support;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared registry of active channels. Each accepted child channel installs its
 * OWN non-sharable {@link PerChannel} handler that reports into this registry;
 * one handler instance must never be added to multiple pipelines.
 */
public final class ChannelRegistry {

    private final Set<Channel> activeChannels = ConcurrentHashMap.newKeySet();

    private void activated(final Channel channel) {
        activeChannels.add(channel);
    }

    private void deactivated(final Channel channel) {
        activeChannels.remove(channel);
    }

    public Set<Channel> activeChannels() {
        return activeChannels;
    }

    public int activeCount() {
        return activeChannels.size();
    }

    /**
     * Per-pipeline reporter. A new instance is created for every accepted channel.
     */
    public static final class PerChannel extends ChannelInboundHandlerAdapter {
        private final ChannelRegistry registry;

        public PerChannel(final ChannelRegistry registry) {
            this.registry = registry;
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) throws Exception {
            registry.activated(ctx.channel());
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
            registry.deactivated(ctx.channel());
            super.channelInactive(ctx);
        }

        @Override
        public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
            registry.deactivated(ctx.channel());
            super.channelUnregistered(ctx);
        }
    }
}

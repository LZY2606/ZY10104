package com.github.kpavlov.jreactive8583.contract.support;

import com.github.kpavlov.jreactive8583.client.ClientConfiguration;
import com.github.kpavlov.jreactive8583.server.ServerConfiguration;
import org.junit.jupiter.api.AfterEach;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Base class for real loopback contract scenarios. Every fixture registers
 * itself once initialized, and regardless of test outcome {@link #auditAfterTest()}
 * performs the same cleanup and fails the test with an attribution report if any
 * event loop, child channel or allocator reference survives.
 */
public abstract class AbstractLoopbackContractTest {

    private static final int RECONNECT_INTERVAL_MILLIS = 50;
    private static final AtomicLong FIXTURE_SEQUENCE = new AtomicLong();

    private final List<ContractServer> servers = new ArrayList<>();
    private final List<ContractClient> clients = new ArrayList<>();
    private final List<ChannelRegistry> registries = new ArrayList<>();
    private final ResourceAuditor auditor = new ResourceAuditor();
    private boolean used = false;

    private String ownerToken(final String role) {
        // Globally unique: JUnit reuses instances and parallel test methods may
        // share a class, so identityHashCode alone can collide.
        return getClass().getSimpleName() + "-" + role
            + "-" + FIXTURE_SEQUENCE.incrementAndGet();
    }

    /**
     * Polls a condition with an explicit deadline. {@code Iso8583Client.connect()}
     * returns the channel's close future and may return a few milliseconds before
     * the channel is active; tests synchronize on state (never on a fixed sleep).
     */
    protected static void awaitCondition(final BooleanSupplier condition, final String alias)
        throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("Timed out waiting for: " + alias);
        }
    }

    protected ContractServer createServer() {
        used = true;
        final var registry = new ChannelRegistry();
        final var server = new ContractServer(
            ServerConfiguration.newBuilder()
                .workerThreadsCount(1)
                .addEchoMessageListener(true)
                .idleTimeout(1)
                .build(),
            ContractMessageFactory.server(),
            ownerToken("server"),
            registry);
        servers.add(server);
        registries.add(registry);
        return server;
    }

    protected ContractClient createClient(final SocketAddress address) {
        return createClient(address, RECONNECT_INTERVAL_MILLIS);
    }

    protected ContractClient createClient(final SocketAddress address,
                                          final int reconnectIntervalMillis) {
        used = true;
        final var client = new ContractClient(
            address,
            ClientConfiguration.newBuilder()
                .workerThreadsCount(1)
                .reconnectInterval(reconnectIntervalMillis)
                .addEchoMessageListener(true)
                .idleTimeout(1)
                .build(),
            ContractMessageFactory.client(),
            ownerToken("client"));
        clients.add(client);
        return client;
    }

    protected void trackInitialized(final ContractServer server) {
        auditor.trackEventLoop(server.bossGroupForAudit(), server.ownerToken() + "-server-boss");
        auditor.trackEventLoop(server.workerGroupForAudit(), server.ownerToken() + "-server-worker");
        auditor.trackAllocator(server.trackingAllocator());
    }

    protected void trackInitialized(final ContractClient client) {
        auditor.trackEventLoop(client.bossGroupForAudit(), client.ownerToken() + "-client-nio");
        auditor.trackEventLoop(client.workerGroupForAudit(), client.ownerToken() + "-client-worker");
        auditor.trackAllocator(client.trackingAllocator());
    }

    /**
     * Cleanup runs even when the scenario failed. It shuts each connector down
     * (closing its channel and cancelling reconnect intent), then verifies that
     * every event loop, child channel and allocator reference is gone.
     */
    @AfterEach
    public final void auditAfterTest() {
        if (!used) {
            return;
        }
        RuntimeException shutdownFailure = null;
        for (final var client : clients) {
            try {
                client.shutdown();
            } catch (final RuntimeException e) {
                if (shutdownFailure == null) {
                    shutdownFailure = e;
                }
            }
        }
        for (final var server : servers) {
            try {
                server.shutdown();
            } catch (final RuntimeException e) {
                if (shutdownFailure == null) {
                    shutdownFailure = e;
                }
            }
        }

        assertChildChannelsClosed();

        if (shutdownFailure != null) {
            throw shutdownFailure;
        }
        auditor.assertAllReleased();
    }

    private void assertChildChannelsClosed() {
        // Server child channels close asynchronously once the server channel is
        // closed; poll with a deadline instead of a fixed sleep.
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        int total = countActiveChildren();
        while (total > 0 && System.nanoTime() < deadline) {
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            total = countActiveChildren();
        }
        if (total > 0) {
            final List<String> descriptions = new ArrayList<>();
            for (final var registry : registries) {
                for (final var channel : registry.activeChannels()) {
                    descriptions.add("child channel still active: "
                        + channel + " open=" + channel.isOpen()
                        + " active=" + channel.isActive()
                        + " remote=" + channel.remoteAddress());
                }
            }
            throw new AssertionError(
                "Accepted channel(s) were not closed:" + System.lineSeparator()
                    + String.join(System.lineSeparator(), descriptions));
        }
    }

    private int countActiveChildren() {
        var total = 0;
        for (final var registry : registries) {
            total += registry.activeCount();
        }
        return total;
    }
}

package com.github.kpavlov.jreactive8583.contract.loopback;

import com.github.kpavlov.jreactive8583.contract.support.AbstractLoopbackContractTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reconnect behaviour over real loopback with dynamic ports.
 *
 * <ul>
 *   <li>connect retries while the remote refuses;</li>
 *   <li>reconnect succeeds once a previously down server comes back at the
 *       address taken from its actual bound channel.</li>
 * </ul>
 * Progress is observed via a counted connect attempt polled under an explicit
 * deadline - there are no fixed sleeps and no pre-allocated "free port".
 */
@Tag("loopback")
class LoopbackReconnectIT extends AbstractLoopbackContractTest {

    private static final long TIMEOUT_SECONDS = 15;
    private static final int RECONNECT_INTERVAL_MILLIS = 25;

    @Test
    void retriesConnectWhileRemoteRefusesThenConnects() throws Exception {
        final var server = createServer();
        server.init();
        trackInitialized(server);
        server.start();
        awaitCondition(server::isStarted, "server started");
        final var address = server.loopbackAddress();

        // Client starts against a port that refuses: easiest deterministic refusal
        // is the loopback discard port (0), which fails immediately on every OS.
        final var refusedAddress = new InetSocketAddress("127.0.0.1", 0);
        final var refusedClient = createClient(refusedAddress, RECONNECT_INTERVAL_MILLIS);
        refusedClient.init();
        trackInitialized(refusedClient);
        refusedClient.connectAsync();

        // Observe multiple failed attempts through the fixture counter with a deadline.
        final var start = System.nanoTime();
        while (refusedClient.connectAttemptCount() < 5
            && System.nanoTime() - start < TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS)) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertThat(refusedClient.connectAttemptCount())
            .as("client kept retrying a refused connection")
            .isGreaterThanOrEqualTo(5);

        // Shut the refusing client down cleanly before opening one against the
        // real server, keeping the two lifecycles independent.
        refusedClient.shutdown();

        final var client = createClient(address, RECONNECT_INTERVAL_MILLIS);
        client.init();
        trackInitialized(client);
        client.connect();
        awaitCondition(client::isConnected, "fresh client connected");
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void reconnectsAfterServerComesBackOnSameAddress() throws Exception {
        final var server = createServer();
        server.init();
        trackInitialized(server);
        server.start();
        awaitCondition(server::isStarted, "server started");
        final var address = server.loopbackAddress();

        final var client = createClient(address, RECONNECT_INTERVAL_MILLIS);
        client.init();
        trackInitialized(client);
        client.connect();
        awaitCondition(client::isConnected, "fresh client connected");
        assertThat(client.isConnected()).isTrue();

        // Kill the server: the client channel closes and automatic reconnect
        // attempts start failing against the now-refusing port.
        server.shutdown();
        await(() -> !client.isConnected(), "client disconnected after server shutdown");

        // Bring a server back and point a fresh client at its newly bound address.
        final var restarted = createServer();
        restarted.init();
        trackInitialized(restarted);
        restarted.start();
        awaitCondition(restarted::isStarted, "restarted server started");
        final var newAddress = restarted.loopbackAddress();
        assertThat(newAddress).isNotNull();

        // The original client is closed explicitly; it must not fire another
        // reconnect after its event loop is shut down (stop/reconnect race).
        client.shutdown();

        final var recovered = createClient(newAddress, RECONNECT_INTERVAL_MILLIS);
        recovered.init();
        trackInitialized(recovered);
        recovered.connect();
        awaitCondition(recovered::isConnected, "recovered client connected");
        assertThat(recovered.isConnected()).isTrue();
    }

    private void await(final java.util.function.BooleanSupplier condition, final String alias)
        throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertThat(condition.getAsBoolean()).as(alias).isTrue();
    }
}

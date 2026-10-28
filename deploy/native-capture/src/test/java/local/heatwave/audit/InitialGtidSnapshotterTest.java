package local.heatwave.audit;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.junit.Test;
import io.debezium.spi.snapshot.Snapshotter;

public class InitialGtidSnapshotterTest {
    @Test
    public void capturesNeitherRowsNorStreamBeforeInitializationIsVerified() {
        Snapshotter snapshotter = new InitialGtidSnapshotter();
        assertFalse(snapshotter.shouldSnapshotData(false, false));
        assertFalse(snapshotter.shouldSnapshotData(true, true));
        assertFalse(snapshotter.shouldStream());
        assertFalse(snapshotter.shouldStreamEventsStartingFromSnapshot());
        assertFalse(snapshotter.shouldSnapshotOnDataError());
        assertFalse(snapshotter.shouldSnapshotOnSchemaError());
    }

    @Test
    public void debeziumCanDiscoverThePackagedSnapshotter() {
        assertTrue(ServiceLoader.load(Snapshotter.class).stream()
            .anyMatch(provider -> provider.type() == InitialGtidSnapshotter.class));
    }

    @Test
    public void rejectsMissingOrUnsafeCompletionToken() {
        Snapshotter snapshotter = new InitialGtidSnapshotter();
        assertThrows(IllegalArgumentException.class, () -> snapshotter.configure(Map.of()));
        assertThrows(IllegalArgumentException.class,
            () -> snapshotter.configure(Map.of("audit.initial.token", "../../invalid")));
    }

    @Test
    public void signalsOnlyTheCurrentInitializationAfterSchemaCompletion() throws Exception {
        String token = UUID.randomUUID().toString().replace("-", "");
        Path folder = Path.of("/tmp/audit-runtime");
        Files.createDirectories(folder);
        Path marker = folder.resolve("schema-ready-" + token);
        Snapshotter snapshotter = new InitialGtidSnapshotter();
        snapshotter.configure(Map.of("audit.initial.token", token, "topic.prefix", "audit_test"));
        assertFalse(Files.exists(marker));
        try {
            snapshotter.snapshotCompleted();
            assertEquals(token, Files.readString(marker));
        }
        finally {
            Files.deleteIfExists(marker);
        }
    }

    public interface CompletionMXBean {
        boolean getSnapshotCompleted();
    }

    public static final class Completion implements CompletionMXBean {
        boolean completed;
        @Override
        public boolean getSnapshotCompleted() {
            return completed;
        }
    }

    @Test
    public void schemaOnlyCompletionUsesTheMatchingSourceMetricAndNeverStreams() throws Exception {
        String token = UUID.randomUUID().toString().replace("-", "");
        Path marker = Path.of("/tmp/audit-runtime/schema-ready-" + token);
        Files.createDirectories(marker.getParent());
        String source = "audit_test_" + token;
        MBeanServer metrics = ManagementFactory.getPlatformMBeanServer();
        ObjectName own = new ObjectName("debezium.mysql:type=connector-metrics,context=snapshot,server=" + source);
        ObjectName other = new ObjectName("debezium.mysql:type=connector-metrics,context=snapshot,server=other_" + token);
        Completion current = new Completion();
        Completion unrelated = new Completion();
        unrelated.completed = true;
        metrics.registerMBean(new StandardMBean(current, CompletionMXBean.class), own);
        metrics.registerMBean(new StandardMBean(unrelated, CompletionMXBean.class), other);
        try {
            Snapshotter snapshotter = new InitialGtidSnapshotter();
            snapshotter.configure(Map.of("audit.initial.token", token, "topic.prefix", source));
            assertFalse(snapshotter.shouldStream());
            assertFalse(Files.exists(marker));
            current.completed = true;
            assertFalse(snapshotter.shouldStream());
            assertEquals(token, Files.readString(marker));
        }
        finally {
            metrics.unregisterMBean(own);
            metrics.unregisterMBean(other);
            Files.deleteIfExists(marker);
        }
    }
}

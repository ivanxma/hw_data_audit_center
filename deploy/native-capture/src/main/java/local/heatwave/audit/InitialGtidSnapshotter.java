package local.heatwave.audit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.lang.management.ManagementFactory;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.debezium.snapshot.mode.NoDataSnapshotter;

/** Capture schema at a seeded offset, without reading or streaming row data. */
public final class InitialGtidSnapshotter extends NoDataSnapshotter {
    private String token;
    private String serverName;

    @Override
    public String name() {
        return "audit_initial_gtid";
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        Object value = properties.get("audit.initial.token");
        if (!(value instanceof String) || !((String) value).matches("[a-f0-9]{32}")) {
            throw new IllegalArgumentException("Initial GTID initialization token is required.");
        }
        token = (String) value;
        Object prefix = properties.get("topic.prefix");
        if (!(prefix instanceof String) || ((String) prefix).isBlank()) {
            throw new IllegalArgumentException("Initial GTID source topic prefix is required.");
        }
        serverName = (String) prefix;
    }

    @Override
    public boolean shouldStream() {
        // Schema-only snapshots do not invoke the Snapshotter completion hook
        // through the row-event dispatcher. The coordinator calls shouldStream
        // after the snapshot metrics have been marked complete as well as before
        // startup, so only the matching completed snapshot can release bootstrap.
        if (token != null && serverName != null) {
            MBeanServer metrics = ManagementFactory.getPlatformMBeanServer();
            try {
                for (ObjectName name : metrics.queryNames(
                        new ObjectName("debezium.mysql:type=connector-metrics,context=snapshot,*"), null)) {
                    String source = name.getKeyProperty("server");
                    if (source != null && source.startsWith("\"")) {
                        source = ObjectName.unquote(source);
                    }
                    if (serverName.equals(source) && Boolean.TRUE.equals(metrics.getAttribute(name, "SnapshotCompleted"))) {
                        snapshotCompleted();
                    }
                }
            }
            catch (JMException error) {
                throw new IllegalStateException("Cannot verify initial GTID schema completion.", error);
            }
        }
        return false;
    }

    @Override
    public boolean shouldStreamEventsStartingFromSnapshot() {
        return false;
    }

    @Override
    public void snapshotCompleted() {
        try {
            Files.writeString(Path.of("/tmp/audit-runtime/schema-ready-" + token), token);
        }
        catch (IOException error) {
            throw new IllegalStateException("Cannot confirm initial GTID schema capture.", error);
        }
    }
}

package local.heatwave.audit;

import java.util.*;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.spi.snapshot.Snapshotter;

/** Offline verification of the shaded image's engine and service registrations. */
public final class RuntimeCheck {
    public static void main(String[] args) throws Exception {
        jakarta.ws.rs.ext.RuntimeDelegate.getInstance();
        Class.forName("com.mysql.cj.jdbc.Driver");
        Class.forName("io.debezium.storage.jdbc.history.JdbcSchemaHistory");
        Class.forName("com.oracle.bmc.streaming.StreamClient");
        if(DebeziumEngine.create(Json.class)==null) throw new IllegalStateException("Missing engine factory");
        boolean found=false;
        for(var provider:ServiceLoader.load(Snapshotter.class)) {
            if(provider instanceof InitialGtidSnapshotter) found=true;
        }
        if(!found) throw new IllegalStateException("Missing initial GTID snapshotter");
        var decoded=NativeOffsetStore.decode(NativeOffsetStore.encode(Map.of("pos",123L)));
        if(!decoded.equals(Map.of("pos",123L))) throw new IllegalStateException("Offset serialization mismatch");
        System.out.println("Native capture engine, JDBC, OCI publisher, offset serializer and GTID SPI verified.");
    }
}

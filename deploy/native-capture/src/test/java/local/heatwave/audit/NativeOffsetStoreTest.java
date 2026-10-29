package local.heatwave.audit;

import static org.junit.Assert.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.sql.DriverManager;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.junit.*;

public class NativeOffsetStoreTest {
    String jdbcUrl, username, password, schema;
    JsonNode previousSettings;
    @Before public void setup() throws Exception {
        jdbcUrl=System.getenv("AUDIT_TEST_MYSQL_JDBC_URL");
        Assume.assumeTrue("Set AUDIT_TEST_MYSQL_JDBC_URL for disposable MySQL integration tests",
            jdbcUrl!=null && !jdbcUrl.isBlank());
        assertTrue("Integration tests require a MySQL JDBC URL",jdbcUrl.startsWith("jdbc:mysql:"));
        String credentials=System.getenv("AUDIT_TEST_MYSQL_CREDENTIALS_FILE");
        assertNotNull("Set AUDIT_TEST_MYSQL_CREDENTIALS_FILE",credentials);
        var secrets=new Properties();
        try(var input=Files.newInputStream(Path.of(credentials))) {secrets.load(input);}
        username=secrets.getProperty("username");password=secrets.getProperty("history_password");
        assertNotNull("Credential file must define username",username);
        assertNotNull("Credential file must define history_password",password);
        previousSettings=NativeSettings.settings;
        // Only this generated schema is created or removed; never reuse a configured schema.
        String candidate="audit_offset_test_"+UUID.randomUUID().toString().replace("-", "");
        try(var db=DriverManager.getConnection(jdbcUrl,username,password);var sql=db.createStatement()) {
            try(var rows=sql.executeQuery("SELECT @@SESSION.sql_mode")) {
                assertTrue(rows.next());
                String mode=rows.getString(1);
                assertTrue("MySQL tests require strict SQL mode for overflow rollback checks",
                    mode.contains("STRICT_TRANS_TABLES") || mode.contains("STRICT_ALL_TABLES"));
            }
            sql.execute("CREATE DATABASE "+NativeSettings.identifier(candidate));
        }
        schema=candidate;
        var settings=NativeSettings.JSON.createObjectNode();
        settings.put("credentials_file",credentials);
        settings.put("connector_name","capture-test");
        settings.put("state_jdbc_url",jdbcUrl);
        var control=settings.putObject("capture_control"); control.put("flow_id",123);
        var target=control.putObject("target");target.put("database",schema);target.put("username",username);
        settings.putObject("connector").put("topic.prefix","audit_123");
        NativeSettings.settings=settings;
    }
    @After public void cleanup() throws Exception {
        if(schema==null) return;
        try(var db=DriverManager.getConnection(jdbcUrl,username,password);var sql=db.createStatement()) {
            sql.execute("DROP DATABASE "+NativeSettings.identifier(schema));
        } finally {NativeSettings.settings=previousSettings;}
    }
    @Test public void committedOffsetsSurviveStoreRestartWithAllCoordinates() throws Exception {
        var store=new NativeOffsetStore();store.start();
        var partition=Map.of("server","audit_123");
        var offset=Map.<String,Object>of("file","binlog.000003","pos",1290L,"row",2L,"event",3L,
            "gtids","aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-32,bbbbbbbb-cccc-dddd-eeee-ffffffffffff:1-9:12-15");
        store.patch(List.of(Map.of("partition",partition,"offset",offset)));store.stop();
        var recovered=new NativeOffsetStore();recovered.start();
        assertEquals(List.of(Map.of("partition",partition,"offset",offset)),recovered.documents());
        assertEquals(Set.of(partition),recovered.connectorPartitions("capture-test"));
        assertTrue(recovered.connectorPartitions("another-capture").isEmpty());
        NativeSettings.settings.withObject("/capture_control").put("flow_id",124);
        assertTrue(recovered.documents().isEmpty());
    }
    @Test public void failedTransactionRollsBackEntireBatchBeforeCallback() throws Exception {
        var store=new NativeOffsetStore();store.start();
        var key=ByteBuffer.wrap(NativeOffsetStore.encode(List.of("capture-test",Map.of("server","audit_123"))));
        var value=ByteBuffer.wrap(NativeOffsetStore.encode(Map.of("pos",10L)));
        var updates=new LinkedHashMap<ByteBuffer,ByteBuffer>();updates.put(key,value);
        updates.put(ByteBuffer.wrap(new byte[2049]),value);
        var callbacks=new ArrayList<Throwable>();
        assertThrows(ExecutionException.class,()->store.set(updates,(error,ignored)->callbacks.add(error)).get());
        assertEquals(1,callbacks.size());assertNotNull(callbacks.get(0));assertTrue(store.all().isEmpty());
    }
    @Test public void wrongSourcePartitionCannotBeSeeded() throws Exception {
        var store=new NativeOffsetStore();store.start();
        assertThrows(IllegalArgumentException.class,()->store.patch(List.of(Map.of(
            "partition",Map.of("server","audit_999"),"offset",Map.of("pos",10L)))));
        assertTrue(store.all().isEmpty());
    }
}

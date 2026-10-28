package local.heatwave.audit;

import static org.junit.Assert.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.junit.*;

public class NativeOffsetStoreTest {
    Path credentials;
    @Before public void setup() throws Exception {
        credentials=Files.createTempFile("native-offset-test-", ".properties");
        Files.writeString(credentials,"history_password=\n");
        var settings=NativeSettings.JSON.createObjectNode();
        settings.put("credentials_file",credentials.toString());
        settings.put("connector_name","capture-test");
        settings.put("state_jdbc_url","jdbc:h2:mem:"+UUID.randomUUID()+";MODE=MySQL;DB_CLOSE_DELAY=-1");
        var control=settings.putObject("capture_control"); control.put("flow_id",123);
        var target=control.putObject("target");target.put("database","PUBLIC");target.put("username","sa");
        settings.putObject("connector").put("topic.prefix","audit_123");
        NativeSettings.settings=settings;
    }
    @After public void cleanup() throws Exception {Files.deleteIfExists(credentials);}
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

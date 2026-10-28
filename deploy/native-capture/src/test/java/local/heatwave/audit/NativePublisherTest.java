package local.heatwave.audit;

import static org.junit.Assert.*;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import java.util.*;
import org.junit.Test;

public class NativePublisherTest {
    record Event(String destination,String key,String value) implements ChangeEvent<String,String> {
        @Override public Integer partition() { return null; }
    }
    static final class Committer implements DebeziumEngine.RecordCommitter<ChangeEvent<String,String>> {
        final List<ChangeEvent<String,String>> processed=new ArrayList<>(); boolean finished;
        public void markProcessed(ChangeEvent<String,String> record) { processed.add(record); }
        public void markProcessed(ChangeEvent<String,String> record,DebeziumEngine.Offsets offsets) { processed.add(record); }
        public void markBatchFinished() { finished=true; }
        public DebeziumEngine.Offsets buildOffsets() { throw new UnsupportedOperationException(); }
    }
    @Test public void bothStreamsMustPublishBeforeAnySourceAcknowledgement() throws Exception {
        var committer=new Committer(); List<String> calls=new ArrayList<>();
        var publisher=new StreamingPublisher(Map.of("data","data-id","schema","schema-id"),(stream,records)->{
            assertTrue(committer.processed.isEmpty()); calls.add(stream);
            assertEquals('{',records.get(0).getValue()[0]);
        });
        var records=List.<ChangeEvent<String,String>>of(new Event("data","{}","{}"),new Event("schema","{}","{}"));
        publisher.handleBatch(records,committer);
        assertEquals(List.of("data-id","schema-id"),calls);
        assertEquals(records,committer.processed); assertTrue(committer.finished);
    }
    @Test public void aLaterPartialFailureCannotAdvancePastAnUnpublishedRecord() {
        var committer=new Committer();
        var publisher=new StreamingPublisher(Map.of("data","data-id","schema","schema-id"),(stream,records)->{
            if(stream.equals("schema-id"))throw new IllegalStateException("partial publish");
        });
        assertThrows(IllegalStateException.class,()->publisher.handleBatch(
            List.of(new Event("data","{}","{}"),new Event("schema","{}","{}")),committer));
        assertTrue(committer.processed.isEmpty()); assertFalse(committer.finished);
    }
    @Test public void eventsAboveLogicalMaximumFailBeforeAcknowledgement() {
        var committer=new Committer(); List<String> calls=new ArrayList<>();
        var publisher=new StreamingPublisher(Map.of("data","data-id"),(stream,records)->calls.add(stream));
        assertThrows(IllegalStateException.class,()->publisher.handleBatch(
            List.of(new Event("data","{}","x".repeat(MessageFraming.DEFAULT_EVENT_BYTES+1))),committer));
        assertTrue(calls.isEmpty()); assertTrue(committer.processed.isEmpty());
    }
    @Test public void oversizedEventsPublishAllFragmentsBeforeAcknowledging() throws Exception {
        var committer=new Committer();List<com.oracle.bmc.streaming.model.PutMessagesDetailsEntry> delivered=new ArrayList<>();
        var publisher=new StreamingPublisher(Map.of("data","data-id"),(stream,records)->{
            assertTrue(committer.processed.isEmpty());
            for(var record:records)assertTrue(MessageFraming.wireSize(record.getKey(),record.getValue())<=MessageFraming.PUBLISH_BYTES);
            delivered.addAll(records);
        });
        publisher.handleBatch(List.of(new Event("data","{\"id\":1}","界".repeat(400000))),committer);
        assertTrue(delivered.size()>1);assertEquals(1,committer.processed.size());assertTrue(committer.finished);
    }
    @Test public void aFailedFragmentCannotAdvanceTheSourceOffset() {
        var committer=new Committer();int[] calls={0};
        var publisher=new StreamingPublisher(Map.of("data","data-id"),(stream,records)->{
            if(++calls[0]==2)throw new IllegalStateException("second fragment failed");
        });
        assertThrows(IllegalStateException.class,()->publisher.handleBatch(List.of(new Event("data","{}","x".repeat(1200000))),committer));
        assertEquals(2,calls[0]);assertTrue(committer.processed.isEmpty());assertFalse(committer.finished);
    }
    @Test public void unknownDestinationsCannotBeSilentlyDiscarded() {
        var committer=new Committer();
        var publisher=new StreamingPublisher(Map.of("data","data-id"),(stream,records)->{});
        assertThrows(IllegalStateException.class,()->publisher.handleBatch(
            List.of(new Event("unregistered","{}","{}")),committer));
        assertFalse(committer.finished);
    }
    @Test public void offsetSerializationPreservesAllGtidGroupsAndRestartCoordinates() {
        Object key=List.of("flow",Map.of("server","audit_1"));
        Object offset=Map.of("gtids","aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-32,bbbbbbbb-cccc-dddd-eeee-ffffffffffff:1-9:12-15",
            "file","binlog.000001","pos",1090L,"row",2L,"event",3L);
        assertEquals(key,NativeOffsetStore.decode(NativeOffsetStore.encode(key)));
        assertEquals(offset,NativeOffsetStore.decode(NativeOffsetStore.encode(offset)));
    }
}

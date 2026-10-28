package local.heatwave.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** Publish whole bounded batches before allowing Debezium to checkpoint them. */
public final class StreamingPublisher implements DebeziumEngine.ChangeConsumer<ChangeEvent<String,String>>, AutoCloseable {
    public interface Gateway { void publish(String stream, List<PutMessagesDetailsEntry> entries); }
    private final Gateway gateway;
    private final Map<String,String> routes;
    private int maximum=MessageFraming.DEFAULT_EVENT_BYTES,maximumRecords=500;
    private PublishPacer pacer;
    private final AtomicLong requests=new AtomicLong(),accepted=new AtomicLong(),wireBytes=new AtomicLong(),throttled=new AtomicLong(),logical=new AtomicLong();
    public Map<String,Long> metrics() { return Map.of("put_requests",requests.get(),"put_records",accepted.get(),"put_wire_bytes",wireBytes.get(),"throttled_records",throttled.get(),"published_logical_events",logical.get()); }
    public int batchRecords() { return maximumRecords; }
    private final List<StreamClient> clients = new ArrayList<>();
    public StreamingPublisher(Map<String,String> routes, Gateway gateway) { this.routes=routes; this.gateway=gateway; }
    public StreamingPublisher(JsonNode settings) {
        this(settings,ResourcePrincipalAuthenticationDetailsProvider.builder().build());
    }
    StreamingPublisher(JsonNode settings, com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider provider) {
        this.routes=new HashMap<>();
        var policy=settings.path("message_policy");
        maximumRecords=policy.path("put_max_records").asInt(500);
        if(maximumRecords<1||maximumRecords>1000)throw new IllegalArgumentException("Invalid publish batch count.");
        long rate=policy.path("put_bytes_per_second").asLong(850000);
        pacer=new PublishPacer(policy.path("put_requests_per_second").asInt(4),rate);
        maximum=settings.path("message_policy").path("max_event_bytes").asInt(MessageFraming.DEFAULT_EVENT_BYTES);
        if(maximum<1024*1024||maximum>MessageFraming.MAX_EVENT_BYTES)throw new IllegalArgumentException("Invalid logical event limit.");
        Map<String,StreamClient> byStream = new HashMap<>();
        settings.path("streams").fields().forEachRemaining(entry -> {
            var stream=entry.getValue();
            var client=StreamClient.builder().endpoint(stream.path("messages_endpoint").asText()).build(provider);
            clients.add(client); byStream.put(stream.path("id").asText(),client);
            routes.put(entry.getKey(),stream.path("id").asText());
            pacer.streamRate(stream.path("id").asText(),Math.min(rate,850000L*Math.max(1,stream.path("partitions").asInt(1))));
        });
        gateway=(stream,entries)->publish(byStream.get(stream),stream,entries);
    }
    private void publish(StreamClient client,String stream,List<PutMessagesDetailsEntry> records) {
        List<PutMessagesDetailsEntry> pending=records;
        for(int attempt=0;attempt<8;attempt++) {
            long bytes=pending.stream().mapToLong(e->MessageFraming.wireSize(e.getKey(),e.getValue())).sum();
            pacer.await(stream,bytes);requests.incrementAndGet();wireBytes.addAndGet(bytes);
            com.oracle.bmc.streaming.model.PutMessagesResult result;
            try {
                result=client.putMessages(PutMessagesRequest.builder().streamId(stream)
                    .retryConfiguration(com.oracle.bmc.retrier.RetryConfiguration.NO_RETRY_CONFIGURATION)
                    .putMessagesDetails(PutMessagesDetails.builder().messages(pending).build()).build()).getPutMessagesResult();
            } catch(com.oracle.bmc.model.BmcException error) {
                int status=error.getStatusCode();
                if(attempt==7||!(status==429||status==500||status==502||status==503||status==504))throw error;
                throttled.addAndGet(status==429?pending.size():0);backoff(attempt);continue;
            }
            if(result.getEntries()==null||result.getEntries().size()!=pending.size())
                throw new IllegalStateException("OCI_STREAM_INVALID_PUBLISH_RESULT: no source offset acknowledged.");
            List<PutMessagesDetailsEntry> failed=new ArrayList<>();
            for(int i=0;i<pending.size();i++) {
                String error=result.getEntries().get(i).getError();
                if(error==null){accepted.incrementAndGet();continue;}
                if(!error.equals("429") && !error.equalsIgnoreCase("TooManyRequests") && !error.toUpperCase(Locale.ROOT).contains("THROTTL"))
                    throw new IllegalStateException("OCI_STREAM_PARTIAL_PUBLISH code="+error.replaceAll("[^A-Za-z0-9_]", "")+": no source offset acknowledged.");
                throttled.incrementAndGet();failed.add(pending.get(i));
            }
            if(failed.isEmpty()) {
                if(result.getFailures()!=0)throw new IllegalStateException("OCI_STREAM_INVALID_PUBLISH_RESULT");
                return;
            }
            pending=failed;
            if(attempt<7)backoff(attempt);
        }
        throw new IllegalStateException("OCI_STREAM_THROTTLED: retry budget exhausted; source offset retained.");
    }
    private static void backoff(int attempt) {
        try { Thread.sleep(Math.min(2000,500L<<attempt)); }
        catch(InterruptedException e) {Thread.currentThread().interrupt();throw new IllegalStateException("Streaming publish interrupted; source offset retained.",e);}
    }
    @Override public void handleBatch(List<ChangeEvent<String,String>> records,
            DebeziumEngine.RecordCommitter<ChangeEvent<String,String>> committer) throws InterruptedException {
        String stream=null; List<PutMessagesDetailsEntry> entries=new ArrayList<>(); int bytes=0;
        for (var record:records) {
            String destination=record.destination();
            if (destination.startsWith("__debezium-heartbeat.")) continue;
            if (record.value()==null) continue;
            String selected=routes.get(destination);
            if (selected==null) throw new IllegalStateException("Capture event has no registered OCI stream.");
            for(var entry:MessageFraming.entries(destination,record.key(),record.value(),maximum)) {
                int size=MessageFraming.wireSize(entry.getKey(),entry.getValue());
                if (!entries.isEmpty() && (!selected.equals(stream) || bytes+size>MessageFraming.PUBLISH_BYTES || entries.size()>=maximumRecords)) {
                    gateway.publish(stream,List.copyOf(entries)); entries.clear(); bytes=0;
                }
                stream=selected; entries.add(entry); bytes+=size;
            }
        }
        if (!entries.isEmpty()) gateway.publish(stream,List.copyOf(entries));
        // A later stream/batch failure leaves every input record unacknowledged.
        // Replayed DML is deduplicated using immutable source coordinates.
        for (var record:records) committer.markProcessed(record);
        committer.markBatchFinished();
        logical.addAndGet(records.stream().filter(r->r.value()!=null&&!r.destination().startsWith("__debezium-heartbeat.")).count());
    }
    @Override public void close() { clients.forEach(StreamClient::close); }
}

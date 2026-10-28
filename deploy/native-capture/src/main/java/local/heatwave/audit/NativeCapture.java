package local.heatwave.audit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/** Loopback lifecycle control for an embedded Debezium Engine and native OCI publisher. */
public final class NativeCapture {
    private final NativeOffsetStore store=new NativeOffsetStore();
    private final StreamingPublisher publisher;
    private volatile DebeziumEngine<ChangeEvent<String,String>> engine;
    private volatile Thread thread;
    private volatile String state="STOPPED";
    private volatile boolean desiredRunning;
    private volatile Map<String,String> configuration;
    public NativeCapture() { store.start(); publisher=new StreamingPublisher(NativeSettings.settings); }
    private synchronized void stop() throws Exception {
        desiredRunning=false;
        if (engine!=null && thread!=null && thread.isAlive()) {
            try { engine.close(); }
            catch (IllegalStateException error) {
                // A schema-only engine can complete between the liveness check
                // and close(). Its own shutdown has already flushed offsets.
                thread.join(25000);
                if(thread.isAlive()) throw error;
            }
        }
        if (thread!=null) thread.join(25000);
        if (thread!=null && thread.isAlive()) throw new IllegalStateException("CAPTURE_STOP_TIMEOUT");
        engine=null; thread=null; state="STOPPED";
    }
    private synchronized void resume() throws Exception {
        if (thread!=null && thread.isAlive()) return;
        if (configuration==null) throw new IllegalStateException("Capture configuration is unavailable.");
        Properties properties=new Properties(); properties.putAll(configuration);
        properties.setProperty("name",NativeSettings.name());
        properties.setProperty("offset.storage",NativeOffsetStore.class.getName());
        properties.setProperty("offset.flush.interval.ms","1000");
        properties.setProperty("offset.flush.timeout.ms","15000");
        properties.setProperty("record.processing.threads","1");
        properties.setProperty("record.processing.order","ORDERED");
        properties.setProperty("max.batch.size",Integer.toString(publisher.batchRecords()));
        properties.setProperty("max.queue.size","2048");
        properties.setProperty("max.queue.size.in.bytes","67108864");
        properties.setProperty("database.password",NativeSettings.secret("source_password"));
        properties.setProperty("schema.history.internal.jdbc.connection.password",NativeSettings.secret("history_password"));
        desiredRunning=true; state="STARTING";
        engine=DebeziumEngine.create(Json.class).using(properties).notifying(publisher)
            .using(new DebeziumEngine.ConnectorCallback() {
                @Override public void pollingStarted() { if(desiredRunning) state="RUNNING"; }
                @Override public void taskStarted() { if(desiredRunning) state="RUNNING"; }
            }).using((success,message,error)->state=desiredRunning ? "FAILED" : "STOPPED").build();
        thread=new Thread(engine,"native-capture-engine"); thread.start();
    }
    private synchronized void configure(Map<String,String> next, boolean initialStopped) throws Exception {
        boolean run=configuration==null ? !initialStopped : desiredRunning;
        if (configuration!=null && configuration.equals(next)) { if(run) resume(); return; }
        stop(); configuration=Map.copyOf(next); if(run) resume();
    }
    private void respond(HttpExchange request,int code,Object value) throws Exception {
        byte[] body=NativeSettings.JSON.writeValueAsBytes(value);
        request.getResponseHeaders().set("Content-Type","application/json");
        request.sendResponseHeaders(code,body.length); request.getResponseBody().write(body); request.close();
    }
    private void handle(HttpExchange request) {
        try {
            String path=request.getRequestURI().getPath(), method=request.getRequestMethod();
            String root="/capture/"+NativeSettings.name();
            if (path.equals("/capture") && method.equals("GET")) { respond(request,200,List.of(NativeSettings.name())); return; }
            if (path.equals("/capture") && method.equals("POST")) {
                var document=NativeSettings.JSON.readTree(request.getRequestBody());
                if (!NativeSettings.name().equals(document.path("name").asText())) { respond(request,400,Map.of("error","Flow mismatch")); return; }
                configure(NativeSettings.JSON.convertValue(document.path("config"),new TypeReference<Map<String,String>>(){}),
                    "STOPPED".equals(document.path("initial_state").asText()));
                respond(request,201,Map.of("name",NativeSettings.name())); return;
            }
            if (!path.startsWith(root+"/")) { respond(request,404,Map.of("error","Not found")); return; }
            String action=path.substring(root.length());
            if (action.equals("/config") && method.equals("GET")) {
                respond(request,configuration==null?404:200,configuration==null?Map.of("error","Not configured"):configuration); return;
            }
            if (action.equals("/config") && method.equals("PUT")) {
                configure(NativeSettings.JSON.readValue(request.getRequestBody(),new TypeReference<Map<String,String>>(){}),false);
            } else if (action.equals("/stop") && method.equals("PUT")) { stop();
            } else if (action.equals("/resume") && method.equals("PUT")) { resume();
            } else if (action.equals("/status") && method.equals("GET")) {
                respond(request,200,Map.of("connector",Map.of("state",state),"tasks",
                    "STOPPED".equals(state)?List.of():List.of(Map.of("state",state)),"metrics",publisher.metrics())); return;
            } else if (action.equals("/offsets") && method.equals("GET")) {
                respond(request,200,Map.of("offsets",store.documents())); return;
            } else if (action.equals("/offsets") && method.equals("PATCH")) {
                if (!"STOPPED".equals(state)) { respond(request,409,Map.of("error","Stop capture before changing its position")); return; }
                var document=NativeSettings.JSON.readTree(request.getRequestBody());
                store.patch(NativeSettings.JSON.convertValue(document.path("offsets"),new TypeReference<List<Map<String,Object>>>(){}));
            } else { respond(request,404,Map.of("error","Not found")); return; }
            respond(request,200,Map.of("ok",true));
        } catch (Exception error) {
            System.err.println("AUDIT_CAPTURE_CONTROL_FAILURE type="+error.getClass().getSimpleName());
            try { respond(request,500,Map.of("error","Capture control operation failed; checkpoint retained.")); }
            catch(Exception ignored) { request.close(); }
        }
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=1) throw new IllegalArgumentException("Runtime configuration file required.");
        NativeSettings.settings=NativeSettings.JSON.readTree(Files.readString(Path.of(args[0])));
        NativeCapture runtime=new NativeCapture();
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",8083),0);
        server.createContext("/",runtime::handle); server.setExecutor(Executors.newCachedThreadPool()); server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            try { runtime.stop(); } catch(Exception ignored) {}
            runtime.publisher.close(); server.stop(1);
        }));
        new CountDownLatch(1).await();
    }
}

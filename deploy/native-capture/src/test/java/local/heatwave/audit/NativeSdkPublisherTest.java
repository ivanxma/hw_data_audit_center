package local.heatwave.audit;

import static org.junit.Assert.*;
import com.sun.net.httpserver.HttpServer;
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.security.KeyPairGenerator;
import java.util.*;
import org.junit.Test;

/** Exercise the real OCI HTTP provider, signing and binary wire format on loopback. */
public class NativeSdkPublisherTest {
    @Test public void actualSdkPublishesUtf8AsBase64AndChecksEachResult() throws Exception {
        var generator=KeyPairGenerator.getInstance("RSA");generator.initialize(2048);
        String pem="-----BEGIN PRIVATE KEY-----\n"+Base64.getMimeEncoder(64,new byte[]{10})
            .encodeToString(generator.generateKeyPair().getPrivate().getEncoded())+"\n-----END PRIVATE KEY-----\n";
        var provider=SimpleAuthenticationDetailsProvider.builder().tenantId("ocid1.tenancy.oc1..example")
            .userId("ocid1.user.oc1..example").fingerprint("00:00")
            .privateKeySupplier(()->new ByteArrayInputStream(pem.getBytes(java.nio.charset.StandardCharsets.UTF_8))).build();
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        var bodies=new ArrayList<String>();var signed=new ArrayList<Boolean>();
        server.createContext("/",request->{
            bodies.add(new String(request.getRequestBody().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8));
            signed.add(request.getRequestHeaders().getFirst("Authorization").startsWith("Signature "));
            byte[] reply=(bodies.size()==1 ? "{\"failures\":1,\"entries\":[{\"error\":\"429\",\"errorMessage\":\"retry\"}]}" : "{\"failures\":0,\"entries\":[{\"partition\":\"0\",\"offset\":1,\"timestamp\":\"2026-01-01T00:00:00Z\"}]}").getBytes();
            request.getResponseHeaders().set("Content-Type","application/json");
            request.sendResponseHeaders(200,reply.length);request.getResponseBody().write(reply);request.close();
        });server.start();
        try {
            var settings=NativeSettings.JSON.createObjectNode();
            var stream=settings.putObject("streams").putObject("rows");stream.put("id","stream-id");
            stream.put("messages_endpoint","http://127.0.0.1:"+server.getAddress().getPort());
            var committer=new NativePublisherTest.Committer();
            try(var publisher=new StreamingPublisher(settings,provider)) {
                publisher.handleBatch(List.of(new NativePublisherTest.Event("rows","{\"id\":2}","{\"state\":\"café\"}")),committer);
            }
            assertTrue(committer.finished);assertEquals(List.of(true,true),signed);
            assertEquals(bodies.get(0),bodies.get(1));
            var entry=NativeSettings.JSON.readTree(bodies.get(0)).path("messages").get(0);
            assertEquals("{\"id\":2}",new String(Base64.getDecoder().decode(entry.path("key").asText()),java.nio.charset.StandardCharsets.UTF_8));
            assertEquals("{\"state\":\"café\"}",new String(Base64.getDecoder().decode(entry.path("value").asText()),java.nio.charset.StandardCharsets.UTF_8));
        } finally {server.stop(0);}
    }
}

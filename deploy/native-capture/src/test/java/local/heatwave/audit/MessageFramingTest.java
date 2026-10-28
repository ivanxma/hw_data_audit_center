package local.heatwave.audit;
import static org.junit.Assert.*;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.Test;

public class MessageFramingTest {
    @Test public void normalMessagesKeepOneOriginalKeyAndValue() {
        var entries=MessageFraming.entries("rows","{\"id\":1}","{\"after\":{\"name\":\"café\"}}",MessageFraming.DEFAULT_EVENT_BYTES);
        assertEquals(1,entries.size());
        assertEquals("{\"id\":1}",new String(entries.get(0).getKey(),StandardCharsets.UTF_8));
        assertEquals("{\"after\":{\"name\":\"café\"}}",new String(entries.get(0).getValue(),StandardCharsets.UTF_8));
    }
    @Test public void unicodeAndKeysReassembleExactlyAcrossByteBoundaries() throws Exception {
        String key="{\"id\":3}",value="{\"after\":{\"name\":\""+"界é".repeat(250000)+"\"}}";
        var entries=MessageFraming.entries("rows",key,value,MessageFraming.DEFAULT_EVENT_BYTES);
        var packed=new ByteArrayOutputStream();String id=null,checksum=null;
        for(int i=0;i<entries.size();i++) {
            var entry=entries.get(i);assertArrayEquals(key.getBytes(StandardCharsets.UTF_8),entry.getKey());
            assertTrue(MessageFraming.wireSize(entry.getKey(),entry.getValue())<=MessageFraming.PUBLISH_BYTES);
            var frame=NativeSettings.JSON.readTree(entry.getValue()).path("_audit_fragment_v1");
            assertEquals(i,frame.path("index").asInt());assertEquals(entries.size(),frame.path("count").asInt());
            byte[] chunk=Base64.getDecoder().decode(frame.path("data").asText());
            assertEquals(MessageFraming.sha(chunk),frame.path("chunk_sha256").asText());packed.write(chunk);
            if(id==null){id=frame.path("id").asText();checksum=frame.path("sha256").asText();}
            assertEquals(id,frame.path("id").asText());
        }
        byte[] raw=packed.toByteArray();assertEquals(checksum,MessageFraming.sha(raw));
        assertEquals(key.getBytes(StandardCharsets.UTF_8).length,ByteBuffer.wrap(raw).getLong());
        assertEquals(value,new String(Arrays.copyOfRange(raw,8+key.length(),raw.length),StandardCharsets.UTF_8));
        assertEquals(id,NativeSettings.JSON.readTree(MessageFraming.entries("rows",key,value,MessageFraming.DEFAULT_EVENT_BYTES).get(0).getValue()).path("_audit_fragment_v1").path("id").asText());
    }
    @Test public void nullKeysHaveStableFragmentRoutingAndRemainNullInTheBody() throws Exception {
        var entries=MessageFraming.entries("schema",null,"x".repeat(800000),MessageFraming.DEFAULT_EVENT_BYTES);
        assertTrue(entries.size()>1);assertNotNull(entries.get(0).getKey());
        for(var entry:entries)assertArrayEquals(entries.get(0).getKey(),entry.getKey());
        var first=NativeSettings.JSON.readTree(entries.get(0).getValue()).path("_audit_fragment_v1");
        assertEquals(-1,ByteBuffer.wrap(Base64.getDecoder().decode(first.path("data").asText())).getLong());
    }
}

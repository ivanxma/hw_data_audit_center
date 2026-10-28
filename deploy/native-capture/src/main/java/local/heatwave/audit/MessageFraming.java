package local.heatwave.audit;

import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Versioned, lossless application framing. Small records retain their original bytes. */
final class MessageFraming {
    static final int PUBLISH_BYTES=900000, CHUNK_BYTES=400000;
    static final int DEFAULT_EVENT_BYTES=16*1024*1024, MAX_EVENT_BYTES=64*1024*1024;
    static int wireSize(byte[] key,byte[] value) {
        return ((value.length+2)/3)*4+(key==null?0:((key.length+2)/3)*4)+256;
    }
    static String sha(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch(java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    static List<PutMessagesDetailsEntry> entries(String destination,String key,String value,int maximum) {
        byte[] k=key==null?null:key.getBytes(StandardCharsets.UTF_8),v=value.getBytes(StandardCharsets.UTF_8);
        int total=Math.addExact(8,Math.addExact(k==null?0:k.length,v.length));
        if(total>maximum)throw new IllegalStateException("STREAM_EVENT_TOO_LARGE: logical event exceeds configured reassembly limit; no source offset acknowledged.");
        if(wireSize(k,v)<=PUBLISH_BYTES)return List.of(PutMessagesDetailsEntry.builder().key(k).value(v).build());
        byte[] packed=ByteBuffer.allocate(total).putLong(k==null?-1:k.length).put(k==null?new byte[0]:k).put(v).array();
        String checksum=sha(packed);
        String id=sha((destination+"\0"+checksum).getBytes(StandardCharsets.UTF_8));
        // Preserve row-key partition affinity. Very large keys use a stable hash;
        // MySQL primary-key limits normally keep converted keys far below this bound.
        byte[] routing=k!=null&&k.length>0&&k.length<=65536?k:("audit-fragment:"+(k==null?id:sha(k))).getBytes(StandardCharsets.UTF_8);
        int count=(total+CHUNK_BYTES-1)/CHUNK_BYTES;
        List<PutMessagesDetailsEntry> frames=new ArrayList<>(count);
        for(int i=0;i<count;i++) {
            byte[] chunk=Arrays.copyOfRange(packed,i*CHUNK_BYTES,Math.min(total,(i+1)*CHUNK_BYTES));
            var frame=Map.of("_audit_fragment_v1",Map.of("id",id,"index",i,"count",count,
                "total_bytes",total,"sha256",checksum,"chunk_sha256",sha(chunk),
                "data",Base64.getEncoder().encodeToString(chunk)));
            try {
                byte[] bytes=NativeSettings.JSON.writeValueAsBytes(frame);
                if(wireSize(routing,bytes)>PUBLISH_BYTES)throw new IllegalStateException("STREAM_FRAGMENT_TOO_LARGE");
                frames.add(PutMessagesDetailsEntry.builder().key(routing).value(bytes).build());
            } catch(java.io.IOException e) { throw new IllegalStateException("Fragment serialization failed.",e); }
        }
        return frames;
    }
}

package local.heatwave.audit;
import java.util.*;
import java.util.function.LongSupplier;

/** Smooth requests and estimated wire bytes independently for each stream. */
final class PublishPacer {
    interface Sleeper { void sleep(long nanos) throws InterruptedException; }
    private final long requestInterval,defaultRate;
    private final LongSupplier clock;
    private final Sleeper sleeper;
    private final Map<String,Long> nextRequest=new HashMap<>(),nextBytes=new HashMap<>(),rates=new HashMap<>();
    PublishPacer(int requests,long bytes) {
        this(requests,bytes,System::nanoTime,nanos->Thread.sleep(nanos/1000000,(int)(nanos%1000000)));
    }
    PublishPacer(int requests,long bytes,LongSupplier clock,Sleeper sleeper) {
        if(requests<1||requests>20||bytes<65536||bytes>64000000)throw new IllegalArgumentException("Invalid publish pacing policy.");
        requestInterval=(1000000000L+requests-1)/requests;defaultRate=bytes;this.clock=clock;this.sleeper=sleeper;
    }
    void streamRate(String stream,long bytes) { rates.put(stream,bytes); }
    void await(String stream,long bytes) {
        long now=clock.getAsLong(),ready=Math.max(nextRequest.getOrDefault(stream,now),nextBytes.getOrDefault(stream,now));
        while(now<ready) {
            try { sleeper.sleep(ready-now); }
            catch(InterruptedException e) {Thread.currentThread().interrupt();throw new IllegalStateException("Publish pacing interrupted; source checkpoint retained.",e);}
            now=clock.getAsLong();
        }
        long rate=rates.getOrDefault(stream,defaultRate);
        nextRequest.put(stream,now+requestInterval);
        nextBytes.put(stream,now+(bytes*1000000000L+rate-1)/rate);
    }
}

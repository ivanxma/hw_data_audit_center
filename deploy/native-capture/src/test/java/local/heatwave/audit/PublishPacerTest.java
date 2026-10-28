package local.heatwave.audit;
import static org.junit.Assert.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class PublishPacerTest {
    @Test public void requestBudgetAccumulatesSourceRecordsWithoutAcknowledgingEarly() {
        var now=new AtomicLong();var pacer=new PublishPacer(4,850000,now::get,now::addAndGet);
        pacer.await("data",100);assertEquals(0,now.get());
        pacer.await("data",100);assertEquals(250000000,now.get());
        pacer.await("data",100);assertEquals(500000000,now.get());
    }
    @Test public void bytesArePacedEvenWhenRequestCountIsLow() {
        var now=new AtomicLong();var pacer=new PublishPacer(20,850000,now::get,now::addAndGet);
        pacer.await("data",850000);pacer.await("data",100);
        assertEquals(1000000000,now.get());
    }
    @Test public void streamsHaveIndependentBudgetsAndConfiguredPartitionCaps() {
        var now=new AtomicLong();var pacer=new PublishPacer(4,850000,now::get,now::addAndGet);
        pacer.streamRate("data",1700000);
        pacer.await("data",850000);pacer.await("schema",100);assertEquals(0,now.get());
        pacer.await("data",100);assertEquals(500000000,now.get());
    }
}

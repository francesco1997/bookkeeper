package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class DeleteLedgerMetadataTest extends LedgerMetadataInitialization {
    private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private boolean makeModifyBeforeDel;
    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;

    public DeleteLedgerMetadataTest(TestInput testInput) {
        super(testInput.isExist());
        this.ledgerId = testInput.getLedgerId();
        this.exist = testInput.isExist();
        this.makeModifyBeforeDel = testInput.isMakeModifyBeforeDel();
    }


    @Parameterized.Parameters
    public static Collection<TestInput> getTestParameters() {
        List<TestInput> inputs = new ArrayList<>();

        inputs.add(new TestInput((long) -1, false, false));
        inputs.add(new TestInput((long) 0, true, false));
        inputs.add(new TestInput(1L, true, true));

        return inputs;
    }

    private static class TestInput {
        private Long ledgerId;
        private boolean exist;
        private boolean makeModifyBeforeDel;

        public TestInput(Long ledgerId, boolean exist, boolean makeModifyBeforeDel) {
            this.ledgerId = ledgerId;
            this.exist = exist;
            this.makeModifyBeforeDel = makeModifyBeforeDel;
        }

        public Long getLedgerId() {
            return ledgerId;
        }

        public boolean isExist() {
            return exist;
        }

        public boolean isMakeModifyBeforeDel() {
            return makeModifyBeforeDel;
        }
    }

    @Before
    public void setup() throws IOException {
        Map<byte[], byte[]> ledgerDataMap = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(this.ledgerId);
        this.ledgerIdByte = buffer.array();

        if (this.exist) {
            this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.EMPTY).build();
            ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
            super.setLedgerDataMap(ledgerDataMap);
        }

        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), super.getKeyValueStorageFactory(), "fakePath", new NullStatsLogger());
    }

    @Test(expected = Bookie.NoLedgerException.class)
    public void setLedgerMetadataTest() throws Exception {
        DbLedgerStorageDataFormats.LedgerData ledgerData = null;
        Long otherLegerId = this.ledgerId + 1;
        if (this.makeModifyBeforeDel) {
            ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder()
                    .setExists(true)
                    .setFenced(false)
                    .setMasterKey(ByteString.copyFrom("MasterKey".getBytes()))
                    .build();
            this.ledgerMetadataIndex.set(otherLegerId, ledgerData);
            this.ledgerMetadataIndex.set(this.ledgerId, ledgerData);

        }

        this.ledgerMetadataIndex.delete(this.ledgerId);
        this.ledgerMetadataIndex.flush();

        verify(super.getKeyValueStorage(), never()).put(eq(this.ledgerIdByte), any());

        if (this.makeModifyBeforeDel) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(otherLegerId);
            byte[] otherLedgerId = buffer.array();
            verify(super.getKeyValueStorage()).put(otherLedgerId, ledgerData.toByteArray());
        }

        this.ledgerMetadataIndex.removeDeletedLedgers();
        verify(super.getKeyValueStorage()).delete(this.ledgerIdByte);
        this.ledgerMetadataIndex.get(this.ledgerId);
    }
}

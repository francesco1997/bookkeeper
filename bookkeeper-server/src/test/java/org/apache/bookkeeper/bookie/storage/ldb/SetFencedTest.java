package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class SetFencedTest extends LedgerMetadataInitialization{
    private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private boolean fenced;
    private boolean concurrency;

    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public SetFencedTest(TestInput testInput) {
        super(testInput.isExist());
        this.ledgerId = testInput.getLedgerId();
        this.exist = testInput.isExist();
        this.fenced = testInput.isFenced();
        this.concurrency = testInput.isConcurrent();
        Class<? extends Exception> expectedException = testInput.getExpectedException();
        if (expectedException != null) {
            this.expectedException.expect(expectedException);
        }
    }

    @Parameterized.Parameters
    public static Collection<TestInput> getTestParameters() {
        List<TestInput> inputs = new ArrayList<>();

        /*
            ledgerId: {>0}, {=0}, {<0}
            exist: {true}, {false}
            masterKey: {valid}, {empty}
         */
        inputs.add(new TestInput((long) -1, false, false, false, Bookie.NoLedgerException.class));
        inputs.add(new TestInput((long) 0, true, true, false, null));
        inputs.add(new TestInput((long) 1, true,  false,true,null));
        inputs.add(new TestInput((long) 1, true,  false,false,null));

        return inputs;
    }

    private static class TestInput {
        private Long ledgerId;
        private boolean exist;
        private boolean fenced;
        private boolean concurrent;
        private Class<? extends Exception> expectedException;


        public TestInput(Long ledgerId, boolean exist, boolean fenced, boolean concurrent, Class<? extends Exception> expectedException) {
            this.ledgerId = ledgerId;
            this.exist = exist;
            this.fenced = fenced;
            this.concurrent = concurrent;
            this.expectedException = expectedException;
        }

        public Long getLedgerId() {
            return ledgerId;
        }

        public boolean isExist() {
            return exist;
        }

        public boolean isFenced() {
            return fenced;
        }

        public boolean isConcurrent() {
            return concurrent;
        }

        public Class<? extends Exception> getExpectedException() {
            return expectedException;
        }
    }

    @Before
    public void setup() throws IOException {

        if (this.exist) {
            Map<byte[], byte[]> ledgerDataMap = new HashMap<>();
            this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(this.fenced).setMasterKey(ByteString.EMPTY).build();
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(this.ledgerId);
            this.ledgerIdByte = buffer.array();
            ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
            super.setLedgerDataMap(ledgerDataMap);
        }

        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), super.getKeyValueStorageFactory(), "fakePath", new NullStatsLogger());

        if (this.concurrency) {
            this.ledgerMetadataIndex = spy(this.ledgerMetadataIndex);
            when(this.ledgerMetadataIndex.get(this.ledgerId)).then(invocation -> {
                DbLedgerStorageDataFormats.LedgerData ledgerData = (DbLedgerStorageDataFormats.LedgerData) invocation.callRealMethod();
                this.ledgerMetadataIndex.delete(this.ledgerId);
                return ledgerData;
            });
        }

    }

    @Test
    public void setFencedTest() throws Exception {
        boolean returnValue = this.ledgerMetadataIndex.setFenced(this.ledgerId);

        this.ledgerMetadataIndex.flush();

        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertNotNull(actualLedgerData);

        Assert.assertTrue((!this.fenced && returnValue) || (!returnValue && this.fenced));

        Assert.assertTrue(actualLedgerData.getFenced());

        if (returnValue) {
            DbLedgerStorageDataFormats.LedgerData expectedLedgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(true).setMasterKey(ByteString.EMPTY).build();
            verify(super.getKeyValueStorage()).put(this.ledgerIdByte, expectedLedgerData.toByteArray());
        } else {
            verify(super.getKeyValueStorage(), never()).put(any(), any());
        }
    }
}
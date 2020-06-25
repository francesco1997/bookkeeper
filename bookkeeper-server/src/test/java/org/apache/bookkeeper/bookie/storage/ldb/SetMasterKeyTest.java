package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
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

import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class SetMasterKeyTest extends LedgerMetadataInitialization{

    private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private byte[] insertingMasterKey;
    private byte[] ledgerIdByte;
    private byte[] previousMasterKey;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public SetMasterKeyTest(TestInput testInput) {
        super(testInput.isExist());
        this.ledgerId = testInput.getLedgerId();
        this.exist = testInput.isExist();
        this.insertingMasterKey = testInput.getMasterKey();
        this.previousMasterKey = testInput.getMasterKeyPreviousValue();
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
        inputs.add(new TestInput((long) -1, false, "validMasterKey".getBytes(), new byte[0], null));
        inputs.add(new TestInput((long) 0, true, new byte[0], new byte[0], null));
        inputs.add(new TestInput((long) 1, true, "validMasterKey".getBytes(), new byte[0], null));
        inputs.add(new TestInput((long) 1, true, new byte[0], "validMasterKey".getBytes(), null));
        inputs.add(new TestInput((long) 1, true, "anotherMasterKey".getBytes(), "validMasterKey".getBytes(), IOException.class));
        inputs.add(new TestInput((long) 1, true, "validMasterKey".getBytes(), "validMasterKey".getBytes(), null));

        return inputs;
    }

    private static class TestInput {
        private Long ledgerId;
        private boolean exist;
        private byte[] masterKey;
        private byte[] masterKeyPreviousValue;
        private Class<? extends Exception> expectedException;


        public TestInput(Long ledgerId, boolean exist, byte[] masterKey, byte[] masterKeyPreviousValue, Class<? extends Exception> expectedException) {
            this.ledgerId = ledgerId;
            this.exist = exist;
            this.masterKey = masterKey;
            this.masterKeyPreviousValue = masterKeyPreviousValue;
            this.expectedException = expectedException;

        }

        public Long getLedgerId() {
            return ledgerId;
        }

        public boolean isExist() {
            return exist;
        }

        public byte[] getMasterKey() {
            return masterKey;
        }

        public byte[] getMasterKeyPreviousValue() {
            return masterKeyPreviousValue;
        }

        public Class<? extends Exception> getExpectedException() {
            return expectedException;
        }
    }

    @Before
    public void setup() throws IOException {
        Map<byte[], byte[]> ledgerDataMap = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(this.ledgerId);
        this.ledgerIdByte = buffer.array();

        if (this.exist) {
            DbLedgerStorageDataFormats.LedgerData ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.copyFrom(this.previousMasterKey)).build();
            ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
            super.setLedgerDataMap(ledgerDataMap);
        }

        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), super.getKeyValueStorageFactory(), "fakePath", new NullStatsLogger());

    }

    @Test
    public void setMasterKeyTest() throws Exception {
        this.ledgerMetadataIndex.setMasterKey(this.ledgerId, this.insertingMasterKey);
        this.ledgerMetadataIndex.flush();

        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertNotNull(actualLedgerData);

        String newMasterKey = Arrays.toString(this.insertingMasterKey);
        String oldMasterKey = Arrays.toString(this.previousMasterKey);

        String expectedMasterKey = null;
        byte[] expectedMasterKeyBytes = null;
        if (this.previousMasterKey.length == 0 || oldMasterKey.equals(newMasterKey)) {
            expectedMasterKey = newMasterKey;
            expectedMasterKeyBytes = this.insertingMasterKey;
        } else {
            expectedMasterKey = oldMasterKey;
            expectedMasterKeyBytes = this.previousMasterKey;
        }

        Assert.assertEquals(expectedMasterKey, Arrays.toString(actualLedgerData.getMasterKey().toByteArray()));

        DbLedgerStorageDataFormats.LedgerData expectedLegerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.copyFrom(expectedMasterKeyBytes)).build();
        verify(super.getKeyValueStorage()).put(this.ledgerIdByte, expectedLegerData.toByteArray());


    }
}

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
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class SetMasterKeyTest {

    private Map<byte[], byte[]> ledgerDataMap;
    private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private byte[] insertingMasterKey;
    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;
    private byte[] previousMasterKey;

    private FakeKeyValueStorageFactory fakeKeyValueStorageFactory;

    @Mock
    private KeyValueStorage keyValueStorage;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public SetMasterKeyTest(TestInput testInput) {
        this.ledgerId = testInput.getLedgerId();
        this.exist = testInput.isExist();
        this.insertingMasterKey = testInput.getMasterKey();
        this.previousMasterKey = testInput.getMasterKeyPreviousValue();
        Class<? extends Exception> expectedException = testInput.getExpectedException();
        if (expectedException != null) {
            this.expectedException.expect(expectedException);
        }
    }


    private class FakeKeyValueStorageFactory implements KeyValueStorageFactory {
        private KeyValueStorage keyValueStorage;

        public FakeKeyValueStorageFactory(KeyValueStorage keyValueStorage) {
            this.keyValueStorage = keyValueStorage;
        }

        @Override
        public KeyValueStorage newKeyValueStorage(String path, DbConfigType dbConfigType, ServerConfiguration conf) throws IOException {
            return this.keyValueStorage;
        }
    }

    private class FakeClosableIterator<T> implements KeyValueStorage.CloseableIterator<T> {

        private Iterator<T> iterator;

        public FakeClosableIterator(Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() throws IOException {
            return this.iterator.hasNext();
        }

        @Override
        public T next() throws IOException {
            return this.iterator.next();
        }

        @Override
        public void close() throws IOException {

        }
    }

    @Before
    public void configureMock() throws IOException {
        this.fakeKeyValueStorageFactory = new FakeKeyValueStorageFactory(this.keyValueStorage);
        this.ledgerDataMap = new HashMap<>();

        if (this.exist) {
            this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(true).setMasterKey(ByteString.copyFrom(this.previousMasterKey)).build();
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(this.ledgerId);
            this.ledgerIdByte = buffer.array();
            this.ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
        }

        FakeClosableIterator<Map.Entry<byte[], byte[]>> fakeClosableIterator = new FakeClosableIterator<>(ledgerDataMap.entrySet().iterator());
        when(keyValueStorage.iterator()).thenReturn(fakeClosableIterator);
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

    @Test
    public void setMasterKeyTest() throws Exception {
        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), this.fakeKeyValueStorageFactory, "fakePath", new NullStatsLogger());
        ledgerMetadataIndex.setMasterKey(this.ledgerId, this.insertingMasterKey);


        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertNotNull(actualLedgerData);

        String newMasterKey = Arrays.toString(this.insertingMasterKey);
        String oldMasterKey = Arrays.toString(this.previousMasterKey);

        String expectedMasterKey = (this.previousMasterKey.length == 0 || oldMasterKey.equals(newMasterKey)) ? newMasterKey : oldMasterKey;


        Assert.assertEquals(expectedMasterKey, Arrays.toString(actualLedgerData.getMasterKey().toByteArray()));

    }
}

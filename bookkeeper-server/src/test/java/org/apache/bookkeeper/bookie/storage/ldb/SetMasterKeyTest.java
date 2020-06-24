package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
public class SetMasterKeyTest  {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    private Map<byte[], byte[]> ledgerDataMap;
    private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private byte[] masterKey;
    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;


    @Mock
    private KeyValueStorage keyValueStorage;

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

    private FakeKeyValueStorageFactory fakeKeyValueStorageFactory;

    @Before
    public void configureMock() throws IOException {
        this.fakeKeyValueStorageFactory = new FakeKeyValueStorageFactory(this.keyValueStorage);
        this.ledgerDataMap = new HashMap<>();

        if (this.exist) {
            this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(true).setMasterKey(ByteString.EMPTY).build();
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(this.ledgerId);
            this.ledgerIdByte = buffer.array();



            this.ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
        }

        FakeClosableIterator<Map.Entry<byte[], byte[]>> fakeClosableIterator = new FakeClosableIterator<>(ledgerDataMap.entrySet().iterator());
        when(keyValueStorage.iterator()).thenReturn(fakeClosableIterator);
    }

    public SetMasterKeyTest(TestInput testInput) {
        super();
        this.ledgerId = testInput.getLedjerId();
        this.exist = testInput.isExist();
        this.masterKey = testInput.getMasterKey();
    }

    @Parameterized.Parameters
    public static Collection<TestInput> getTestParameters() {
        List<TestInput> inputs = new ArrayList<>();

        /*
            ledgerId: {>0}, {=0}, {<0}
            exist: {true}, {false}
            masterKey: {null}, {valid}, {empty}
         */
        inputs.add(new TestInput((long) -1, false, "validMasterKey".getBytes()));
        inputs.add(new TestInput((long) 0, true, new byte[0]));
        inputs.add(new TestInput((long) 1, true, "validMasterKey".getBytes()));

        return inputs;
    }

    private static class TestInput {
        private Long ledjerId;
        private boolean exist;
        private byte[] masterKey;

        public TestInput(Long ledjerId, boolean exist, byte[] masterKey) {
            this.ledjerId = ledjerId;
            this.exist = exist;
            this.masterKey = masterKey;
        }

        public Long getLedjerId() {
            return ledjerId;
        }
        public boolean isExist() {
            return exist;
        }
        public byte[] getMasterKey() {
            return masterKey;
        }
    }

    @Test
    public void setMasterKeyTest() throws IOException {

        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), this.fakeKeyValueStorageFactory, "fakePath", new NullStatsLogger());

        ledgerMetadataIndex.setMasterKey(this.ledgerId, this.masterKey);

        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertNotNull(actualLedgerData);

        Assert.assertEquals(Arrays.toString(this.masterKey), Arrays.toString(actualLedgerData.getMasterKey().toByteArray()));
    }
}

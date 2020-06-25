package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
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

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class DeleteLedgerMetadataTest {
    private Map<byte[], byte[]> ledgerDataMap;
    private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private boolean makeModifyBeforeDel;
    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;

    private FakeKeyValueStorageFactory fakeKeyValueStorageFactory;

    @Mock
    private KeyValueStorage keyValueStorage;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public DeleteLedgerMetadataTest(TestInput testInput) {
        this.ledgerId = testInput.getLedgerId();
        this.exist = testInput.isExist();
        this.makeModifyBeforeDel = testInput.isMakeModifyBeforeDel();
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
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(1L);
        this.ledgerIdByte = buffer.array();

        if (this.exist) {
            this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.EMPTY).build();
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

    @Test(expected = Bookie.NoLedgerException.class)
    public void setLedgerMetadataTest() throws Exception {
        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), this.fakeKeyValueStorageFactory, "fakePath", new NullStatsLogger());


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
        verify(this.keyValueStorage, never()).put(eq(this.ledgerIdByte), any());

        if (this.makeModifyBeforeDel) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(otherLegerId);
            byte[] otherLedgerId = buffer.array();
            verify(this.keyValueStorage).put(otherLedgerId, ledgerData.toByteArray());
        }

        this.ledgerMetadataIndex.removeDeletedLedgers();
        verify(this.keyValueStorage).delete(this.ledgerIdByte);
        this.ledgerMetadataIndex.get(this.ledgerId);
    }
}

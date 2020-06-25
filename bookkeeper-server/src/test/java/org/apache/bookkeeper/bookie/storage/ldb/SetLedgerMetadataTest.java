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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class SetLedgerMetadataTest {
    private Map<byte[], byte[]> ledgerDataMap;
    private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;

    private FakeKeyValueStorageFactory fakeKeyValueStorageFactory;

    @Mock
    private KeyValueStorage keyValueStorage;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public SetLedgerMetadataTest(TestInput testInput) {
        this.ledgerId = testInput.getLedgerId();
        this.exist = testInput.isExist();
        this.ledgerData = testInput.getLedgerData();
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
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(this.ledgerId);
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
        inputs.add(new TestInput((long) -1, false, null, NullPointerException.class));
        inputs.add(new TestInput(
                (long) 0,
                true,
                DbLedgerStorageDataFormats.LedgerData.newBuilder()
                        .setExists(true)
                        .setFenced(false)
                        .setMasterKey(ByteString.copyFrom("prova".getBytes()))
                        .build(),
                null));
        inputs.add(new TestInput(
                (long) 1,
                false,
                DbLedgerStorageDataFormats.LedgerData.newBuilder()
                        .setExists(true)
                        .setFenced(false)
                        .setMasterKey(ByteString.copyFrom("prova".getBytes()))
                        .build(),
                null));


        return inputs;
    }

    private static class TestInput {
        private Long ledgerId;
        private boolean exist;
        private DbLedgerStorageDataFormats.LedgerData ledgerData;
        private Class<? extends Exception> expectedException;

        public TestInput(Long ledgerId, boolean exist, DbLedgerStorageDataFormats.LedgerData ledgerData, Class<? extends Exception> expectedException) {
            this.ledgerId = ledgerId;
            this.exist = exist;
            this.ledgerData = ledgerData;
            this.expectedException = expectedException;

        }

        public Long getLedgerId() {
            return ledgerId;
        }

        public boolean isExist() {
            return exist;
        }

        public DbLedgerStorageDataFormats.LedgerData getLedgerData() {
            return ledgerData;
        }

        public Class<? extends Exception> getExpectedException() {
            return expectedException;
        }
    }

    @Test
    public void setLedgerMetadataTest() throws Exception {
        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), this.fakeKeyValueStorageFactory, "fakePath", new NullStatsLogger());
        this.ledgerMetadataIndex.set(this.ledgerId, this.ledgerData);

        this.ledgerMetadataIndex.flush();

        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertEquals(Arrays.toString(this.ledgerData.toByteArray()), Arrays.toString(actualLedgerData.toByteArray()));

        verify(this.keyValueStorage).put(this.ledgerIdByte, this.ledgerData.toByteArray());
    }
}

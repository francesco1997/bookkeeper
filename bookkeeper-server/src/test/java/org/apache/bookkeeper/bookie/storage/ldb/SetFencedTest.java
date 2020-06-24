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
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class SetFencedTest {

    private Map<byte[], byte[]> ledgerDataMap;
    private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private boolean fenced;
    private boolean concurrency;

    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;

    private FakeKeyValueStorageFactory fakeKeyValueStorageFactory;

    @Mock
    private KeyValueStorage keyValueStorage;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public SetFencedTest(TestInput testInput) {
        this.ledgerId = testInput.getLedgerId();
        this.exist = testInput.isExist();
        this.fenced = testInput.isFenced();
        this.concurrency = testInput.isConcurrent();
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
            this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(this.fenced).setMasterKey(ByteString.EMPTY).build();
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

    @Test
    public void setFencedTest() throws Exception {
        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), this.fakeKeyValueStorageFactory, "fakePath", new NullStatsLogger());

        if (this.concurrency) {
            this.ledgerMetadataIndex = spy(this.ledgerMetadataIndex);
            when(this.ledgerMetadataIndex.get(this.ledgerId)).then(invocation -> {
                DbLedgerStorageDataFormats.LedgerData ledgerData = (DbLedgerStorageDataFormats.LedgerData) invocation.callRealMethod();
                this.ledgerMetadataIndex.delete(this.ledgerId);
                return ledgerData;
            });
        }

        boolean returnValue = this.ledgerMetadataIndex.setFenced(this.ledgerId);

        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertNotNull(actualLedgerData);

        Assert.assertTrue((!this.fenced && returnValue) || (!returnValue && this.fenced));

        Assert.assertTrue(actualLedgerData.getFenced());
    }
}
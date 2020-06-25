package org.apache.bookkeeper.bookie.storage.ldb;

import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.util.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


public abstract class LedgerMetadataInitialization {
    private Map<byte[], byte[]> ledgerDataMap;
    private Iterator<Map.Entry<byte[], byte[]>> iterator;
    private boolean exist;


    @Mock
    private KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> closeableIterator;

    @Mock
    private KeyValueStorageFactory keyValueStorageFactory;

    @Mock
    private KeyValueStorage keyValueStorage;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();


    protected LedgerMetadataInitialization(boolean exist) {
        this.ledgerDataMap = new HashMap<>();
        this.exist = exist;
    }

    @Before
    public void configureMock() throws IOException {

        when(this.keyValueStorageFactory.newKeyValueStorage(any(),any(),any())).thenReturn(this.keyValueStorage);
        when(keyValueStorage.iterator()).then(invocationOnMock -> {
            this.iterator = this.ledgerDataMap.entrySet().iterator();
            return this.closeableIterator;
        });

        if (this.exist) {
            when(this.closeableIterator.hasNext()).then(invocationOnMock -> this.iterator.hasNext());
            when(this.closeableIterator.next()).then(invocationOnMock -> this.iterator.next());
        } else {
            when(this.closeableIterator.hasNext()).thenReturn(false);
        }

    }

    protected void setLedgerDataMap(Map<byte[], byte[]> ledgerDataMap) {
        this.ledgerDataMap = ledgerDataMap;
    }

    protected KeyValueStorageFactory getKeyValueStorageFactory() {
        return keyValueStorageFactory;
    }

    protected KeyValueStorage getKeyValueStorage() {
        return keyValueStorage;
    }
}

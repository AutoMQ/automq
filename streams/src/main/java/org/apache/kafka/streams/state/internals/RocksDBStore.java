/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.AbstractNotifyingBatchingRestoreCallback;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.StateSerdes;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * A persistent key-value store based on RocksDB.
 *
 * Note that the use of array-typed keys is discouraged because they result in incorrect caching behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.
 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class RocksDBStore<K, V> implements KeyValueStore<K, V> {

    private static final int TTL_NOT_USED = -1;

    private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
    private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
    private static final long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
    private static final long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
    private static final long BLOCK_SIZE = 4096L;
    private static final int TTL_SECONDS = TTL_NOT_USED;
    private static final int MAX_WRITE_BUFFERS = 3;
    private static final String DB_FILE_DIR = "rocksdb";

    private final String name;
    private final String parentDir;
    private final Set<KeyValueIterator> openIterators = Collections.synchronizedSet(new HashSet<KeyValueIterator>());

    File dbDir;
    private StateSerdes<K, V> serdes;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private RocksDB db;

    // the following option objects will be created in the constructor and closed in the close() method
    private Options options;
    private WriteOptions wOptions;
    private FlushOptions fOptions;

    private volatile boolean prepareForBulkload = false;
    private ProcessorContext internalProcessorContext;
    // visible for testing
    volatile BatchingStateRestoreCallback batchingStateRestoreCallback = null;

    protected volatile boolean open = false;

    RocksDBStore(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this(name, DB_FILE_DIR, keySerde, valueSerde);
    }

    RocksDBStore(String name, String parentDir, Serde<K> keySerde, Serde<V> valueSerde) {
        this.name = name;
        this.parentDir = parentDir;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @SuppressWarnings("unchecked")
    public void openDB(ProcessorContext context) {
        // initialize the default rocksdb options
        final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(BLOCK_CACHE_SIZE);
        tableConfig.setBlockSize(BLOCK_SIZE);

        options = new Options();
        options.setTableFormatConfig(tableConfig);
        options.setWriteBufferSize(WRITE_BUFFER_SIZE);
        options.setCompressionType(COMPRESSION_TYPE);
        options.setCompactionStyle(COMPACTION_STYLE);
        options.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        options.setCreateIfMissing(true);
        options.setErrorIfExists(false);
        options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
        // this is the recommended way to increase parallelism in RocksDb
        // note that the current implementation of setIncreaseParallelism affects the number
        // of compaction threads but not flush threads (the latter remains one). Also
        // the parallelism value needs to be at least two because of the code in
        // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
        // subtracts one from the value passed to determine the number of compaction threads
        // (this could be a bug in the RocksDB code and their devs have been contacted).
        options.setIncreaseParallelism(Math.max(Runtime.getRuntime().availableProcessors(), 2));

        if (prepareForBulkload) {
            options.prepareForBulkLoad();
        }

        wOptions = new WriteOptions();
        wOptions.setDisableWAL(true);

        fOptions = new FlushOptions();
        fOptions.setWaitForFlush(true);

        final Map<String, Object> configs = context.appConfigs();
        final Class<RocksDBConfigSetter> configSetterClass =
                (Class<RocksDBConfigSetter>) configs.get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG);

        if (configSetterClass != null) {
            final RocksDBConfigSetter configSetter = Utils.newInstance(configSetterClass);
            configSetter.setConfig(name, options, configs);
        }
        // we need to construct the serde while opening DB since
        // it is also triggered by windowed DB segments without initialization
        this.serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.dbDir = new File(new File(context.stateDir(), parentDir), this.name);

        try {
            this.db = openDB(this.dbDir, this.options, TTL_SECONDS);
        } catch (IOException e) {
            throw new ProcessorStateException(e);
        }

        open = true;
    }

    public void init(ProcessorContext context, StateStore root) {
        // open the DB dir
        this.internalProcessorContext = context;
        openDB(context);
        this.batchingStateRestoreCallback = new RocksDBBatchingRestoreCallback(this);

        // value getter should always read directly from rocksDB
        // since it is only for values that are already flushed
        context.register(root, false, this.batchingStateRestoreCallback);
    }

    private RocksDB openDB(File dir, Options options, int ttl) throws IOException {
        try {
            if (ttl == TTL_NOT_USED) {
                Files.createDirectories(dir.getParentFile().toPath());
                return RocksDB.open(options, dir.getAbsolutePath());
            } else {
                throw new UnsupportedOperationException("Change log is not supported for store " + this.name + " since it is TTL based.");
                // TODO: support TTL with change log?
                // return TtlDB.open(options, dir.toString(), ttl, false);
            }
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error opening store " + this.name + " at location " + dir.toString(), e);
        }
    }

    // visible for testing
    boolean isPrepareForBulkload() {
        return prepareForBulkload;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized V get(K key) {
        validateStoreOpen();
        byte[] byteValue = getInternal(serdes.rawKey(key));
        return byteValue  == null ? null : serdes.valueFrom(byteValue);
    }

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + this.name + " is currently closed");
        }
    }

    private byte[] getInternal(byte[] rawKey) {
        try {
            return this.db.get(rawKey);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while getting value for key " + serdes.keyFrom(rawKey) +
                    " from store " + this.name, e);
        }
    }

    private void toggleDbForBulkLoading(boolean prepareForBulkload) {

        if (prepareForBulkload) {
            // if the store is not empty, we need to compact to get around the num.levels check
            // for bulk loading
            final String[] sstFileNames = dbDir.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.matches(".*\\.sst");
                }
            });

            if (sstFileNames != null && sstFileNames.length > 0) {
                try {
                    this.db.compactRange(true, 1, 0);
                } catch (RocksDBException e) {
                    throw new ProcessorStateException("Error while range compacting during restoring  store " + this.name, e);
                }

                // we need to re-open with the old num.levels again, this is a workaround
                // until https://github.com/facebook/rocksdb/pull/2740 is merged in rocksdb
                close();
                openDB(internalProcessorContext);
            }
        }

        close();
        this.prepareForBulkload = prepareForBulkload;
        openDB(internalProcessorContext);
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        byte[] rawKey = serdes.rawKey(key);
        byte[] rawValue = serdes.rawValue(value);
        putInternal(rawKey, rawValue);
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    private void restoreAllInternal(Collection<KeyValue<byte[], byte[]>> records) {
        try (WriteBatch batch = new WriteBatch()) {
            for (KeyValue<byte[], byte[]> record : records) {
                if (record.value == null) {
                    batch.remove(record.key);
                } else {
                    batch.put(record.key, record.value);
                }
            }
            db.write(wOptions, batch);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
        }
    }

    private void putInternal(byte[] rawKey, byte[] rawValue) {
        if (rawValue == null) {
            try {
                db.delete(wOptions, rawKey);
            } catch (RocksDBException e) {
                throw new ProcessorStateException("Error while removing key " + serdes.keyFrom(rawKey) +
                        " from store " + this.name, e);
            }
        } else {
            try {
                db.put(wOptions, rawKey, rawValue);
            } catch (RocksDBException e) {
                throw new ProcessorStateException("Error while executing put key " + serdes.keyFrom(rawKey) +
                        " and value " + serdes.keyFrom(rawValue) + " from store " + this.name, e);
            }
        }
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        try (WriteBatch batch = new WriteBatch()) {
            for (KeyValue<K, V> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                final byte[] rawKey = serdes.rawKey(entry.key);
                final byte[] rawValue = serdes.rawValue(entry.value);
                if (rawValue == null) {
                    batch.remove(rawKey);
                } else {
                    batch.put(rawKey, rawValue);
                }
            }
            db.write(wOptions, batch);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while batch writing to store " + this.name, e);
        }

    }

    @Override
    public synchronized V delete(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        V value = get(key);
        put(key, null);
        return value;
    }

    @Override
    public synchronized KeyValueIterator<K, V> range(K from, K to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        validateStoreOpen();

        // query rocksdb
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(name, db.newIterator(), serdes, from, to);
        openIterators.add(rocksDBRangeIterator);

        return rocksDBRangeIterator;
    }

    @Override
    public synchronized KeyValueIterator<K, V> all() {
        validateStoreOpen();
        // query rocksdb
        RocksIterator innerIter = db.newIterator();
        innerIter.seekToFirst();
        final RocksDbIterator rocksDbIterator = new RocksDbIterator(name, innerIter, serdes);
        openIterators.add(rocksDbIterator);
        return rocksDbIterator;
    }

    public synchronized KeyValue<K, V> first() {
        validateStoreOpen();
        
        RocksIterator innerIter = db.newIterator();
        innerIter.seekToFirst();
        KeyValue<K, V> pair = new KeyValue<>(serdes.keyFrom(innerIter.key()), serdes.valueFrom(innerIter.value()));
        innerIter.close();

        return pair;
    }

    public synchronized KeyValue<K, V> last() {
        validateStoreOpen();
        
        RocksIterator innerIter = db.newIterator();
        innerIter.seekToLast();
        KeyValue<K, V> pair = new KeyValue<>(serdes.keyFrom(innerIter.key()), serdes.valueFrom(innerIter.value()));
        innerIter.close();

        return pair;
    }

    /**
     * Return an approximate count of key-value mappings in this store.
     *
     * <code>RocksDB</code> cannot return an exact entry count without doing a
     * full scan, so this method relies on the <code>rocksdb.estimate-num-keys</code>
     * property to get an approximate count. The returned size also includes
     * a count of dirty keys in the store's in-memory cache, which may lead to some
     * double-counting of entries and inflate the estimate.
     *
     * @return an approximate count of key-value mappings in the store.
     */
    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        long value;
        try {
            value = this.db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error fetching property from store " + this.name, e);
        }
        if (isOverflowing(value)) {
            return Long.MAX_VALUE;
        }
        return value;
    }

    private boolean isOverflowing(long value) {
        // RocksDB returns an unsigned 8-byte integer, which could overflow long
        // and manifest as a negative value.
        return value < 0;
    }

    @Override
    public synchronized void flush() {
        if (db == null) {
            return;
        }
        // flush RocksDB
        flushInternal();
    }
    /**
     * @throws ProcessorStateException if flushing failed because of any internal store exceptions
     */
    private void flushInternal() {
        try {
            db.flush(fOptions);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while executing flush from store " + this.name, e);
        }
    }

    @Override
    public synchronized void close() {
        if (!open) {
            return;
        }

        open = false;
        closeOpenIterators();
        options.close();
        wOptions.close();
        fOptions.close();
        db.close();

        options = null;
        wOptions = null;
        fOptions = null;
        db = null;
    }

    private void closeOpenIterators() {
        HashSet<KeyValueIterator> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
        }
        for (KeyValueIterator iterator : iterators) {
            iterator.close();
        }
    }

    private class RocksDbIterator implements KeyValueIterator<K, V> {
        private final String storeName;
        private final RocksIterator iter;
        private final StateSerdes<K, V> serdes;

        private volatile boolean open = true;

        RocksDbIterator(String storeName, RocksIterator iter, StateSerdes<K, V> serdes) {
            this.iter = iter;
            this.serdes = serdes;
            this.storeName = storeName;
        }

        byte[] peekRawKey() {
            return iter.key();
        }

        private KeyValue<K, V> getKeyValue() {
            return new KeyValue<>(serdes.keyFrom(iter.key()), serdes.valueFrom(iter.value()));
        }

        @Override
        public synchronized boolean hasNext() {
            if (!open) {
                throw new InvalidStateStoreException(String.format("RocksDB store %s has closed", storeName));
            }

            return iter.isValid();
        }

        /**
         * @throws NoSuchElementException if no next element exist
         */
        @Override
        public synchronized KeyValue<K, V> next() {
            if (!hasNext())
                throw new NoSuchElementException();

            KeyValue<K, V> entry = this.getKeyValue();
            iter.next();
            return entry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("RocksDB iterator does not support remove()");
        }

        @Override
        public synchronized void close() {
            openIterators.remove(this);
            iter.close();
            open = false;
        }

        @Override
        public K peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return serdes.keyFrom(iter.key());
        }
    }

    private class RocksDBRangeIterator extends RocksDbIterator {
        // RocksDB's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private byte[] rawToKey;

        RocksDBRangeIterator(String storeName, RocksIterator iter, StateSerdes<K, V> serdes, K from, K to) {
            super(storeName, iter, serdes);
            iter.seek(serdes.rawKey(from));
            this.rawToKey = serdes.rawKey(to);
            if (this.rawToKey == null) {
                throw new NullPointerException("RocksDBRangeIterator: RawToKey is null for key " + to);
            }
        }

        @Override
        public synchronized boolean hasNext() {
            return super.hasNext() && comparator.compare(super.peekRawKey(), this.rawToKey) <= 0;
        }
    }

    // not private for testing
    static class RocksDBBatchingRestoreCallback extends AbstractNotifyingBatchingRestoreCallback {

        private final RocksDBStore rocksDBStore;

        RocksDBBatchingRestoreCallback(final RocksDBStore rocksDBStore) {
            this.rocksDBStore = rocksDBStore;
        }

        @Override
        public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
            rocksDBStore.restoreAllInternal(records);
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            rocksDBStore.toggleDbForBulkLoading(true);
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
            rocksDBStore.toggleDbForBulkLoading(false);
        }
    }
}

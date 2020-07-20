/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.ratis.thirdparty.com.google.common.annotations.
    VisibleForTesting;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * RocksDB implementation of ozone metadata store.
 */
public class RocksDBStore implements MetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBStore.class);

  private RocksDB db = null;
  private File dbLocation;
  private WriteOptions writeOptions;
  private DBOptions dbOptions;
  private ObjectName statMBeanName;
  private Map<String, ColumnFamilyHandle> columnMap =
      new ConcurrentHashMap<>();
  private ColumnFamilyOptions defaultColumnOpts;

  public RocksDBStore(File dbFile, DBOptions options,
      ColumnFamilyOptions defaultColumnOpts) throws IOException {
    Preconditions.checkNotNull(dbFile, "DB file location cannot be null");
    RocksDB.loadLibrary();
    dbOptions = options;
    dbLocation = dbFile;
    writeOptions = new WriteOptions();
    this.defaultColumnOpts = defaultColumnOpts;
    try {
      File f = new File(dbLocation.getAbsolutePath());
      boolean newDB = !f.exists();

      List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();

      if (newDB) {
        descriptors.add(new ColumnFamilyDescriptor(
            RocksDB.DEFAULT_COLUMN_FAMILY, defaultColumnOpts));
      } else {
        List<byte[]> columnFamilies =
            RocksDB.listColumnFamilies(
                new Options(), dbLocation.getAbsolutePath());
        for (byte[] column : columnFamilies) {
          descriptors.add(
              new ColumnFamilyDescriptor(column, new ColumnFamilyOptions()));
        }
      }

      List<ColumnFamilyHandle> handles = new ArrayList<>();
      db = RocksDB.open(
          dbOptions, dbLocation.getAbsolutePath(), descriptors, handles);

      for (ColumnFamilyHandle handle : handles) {
        columnMap.put(
            StringUtils.bytes2String(handle.getName()), handle);
      }

      if (dbOptions.statistics() != null) {
        Map<String, String> jmxProperties = new HashMap<String, String>();
        jmxProperties.put("dbName", dbFile.getName());
        statMBeanName = HddsUtils.registerWithJmxProperties(
            "Ozone", "RocksDbStore", jmxProperties,
            RocksDBStoreMBean.create(dbOptions.statistics(), dbFile.getName()));
        if (statMBeanName == null) {
          LOG.warn("jmx registration failed during RocksDB init, db path :{}",
              dbFile.getAbsolutePath());
        }
      }
    } catch (RocksDBException e) {
      String msg = "Failed init RocksDB, db path : " + dbFile.getAbsolutePath()
          + ", " + "exception :" + (e.getCause() == null ?
          e.getClass().getCanonicalName() + " " + e.getMessage() :
          e.getCause().getClass().getCanonicalName() + " " +
              e.getCause().getMessage());
      throw new IOException(msg, e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("RocksDB successfully opened.");
      LOG.debug("[Option] dbLocation= {}", dbLocation.getAbsolutePath());
      LOG.debug("[Option] createIfMissing = {}", options.createIfMissing());
      LOG.debug("[Option] compactionPriority= {}", defaultColumnOpts.compactionStyle());
      LOG.debug("[Option] compressionType= {}", defaultColumnOpts.compressionType());
      LOG.debug("[Option] maxOpenFiles= {}", options.maxOpenFiles());
      LOG.debug("[Option] writeBufferSize= {}", defaultColumnOpts.writeBufferSize());
    }
  }

  public static IOException toIOException(String msg, RocksDBException e) {
    String statusCode = e.getStatus() == null ? "N/A" :
        e.getStatus().getCodeString();
    String errMessage = e.getMessage() == null ? "Unknown error" :
        e.getMessage();
    String output = msg + "; status : " + statusCode
        + "; message : " + errMessage;
    return new IOException(output, e);
  }

  private ColumnFamilyHandle getColumnFamilyHandle(String columnFamilyName)
      throws IOException {
    if (!columnMap.containsKey(columnFamilyName)) {
      throw new IOException(
              "columnMap does not container column:" + columnFamilyName);
    }
    return columnMap.get(columnFamilyName);
  }


  @Override
  public void createCategories(List<byte[]> columnFamilyNames)
      throws IOException {
    try {
      List<ColumnFamilyHandle> handles =
          db.createColumnFamilies(defaultColumnOpts, columnFamilyNames);
      for (int i = 0; i < handles.size(); i++) {
        columnMap.put(
            StringUtils.bytes2String(columnFamilyNames.get(i)), handles.get(i));
      }
    } catch (RocksDBException e) {
      throw toIOException("Fail to createCategories", e);
    }
  }

//  @Override
//  public void put(byte[] key, byte[] value) throws IOException {
//    put(getDefaultColumnFamily(), key, value);
//  }

  @Override
  public void put(String category, byte[] key, byte[] value)
      throws IOException {
    try {
      db.put(getColumnFamilyHandle(category), writeOptions, key, value);
    } catch (RocksDBException e) {
      throw toIOException("Failed to put key-value to metadata store", e);
    }
  }

  @Override
  public void put(byte[] category, byte[] key, byte[] value)
      throws IOException {
    put(StringUtils.bytes2String(category), key, value);
  }

//  @Override
//  public boolean isEmpty() throws IOException {
//    return isEmpty(getDefaultColumnFamily());
//  }

  @Override
  public boolean isEmpty(String category) throws IOException {
    RocksIterator it = null;
    try {
      it = db.newIterator(getColumnFamilyHandle(category));
      it.seekToFirst();
      return !it.isValid();
    } finally {
      if (it != null) {
        it.close();
      }
    }
  }

  @Override
  public boolean isEmpty(byte[] category) throws IOException {
    return isEmpty(StringUtils.bytes2String(category));
  }

//  @Override
//  public byte[] get(byte[] key) throws IOException {
//    return get(getDefaultColumnFamily(), key);
//  }

  @Override
  public byte[] get(String category, byte[] key) throws IOException {
    try {
      return db.get(getColumnFamilyHandle(category), key);
    } catch (RocksDBException e) {
      throw toIOException("Failed to get the value for the given key", e);
    }
  }

  @Override
  public byte[] get(byte[] category, byte[] key) throws IOException {
    return get(StringUtils.bytes2String(category), key);
  }

//  @Override
//  public void delete(byte[] key) throws IOException {
//    delete(getDefaultColumnFamily(), key);
//  }

  @Override
  public void delete(String category, byte[] key) throws IOException {
    try {
      db.delete(getColumnFamilyHandle(category), key);
    } catch (RocksDBException e) {
      throw toIOException("Failed to delete the given key", e);
    }
  }

  @Override
  public void delete(byte[] category, byte[] key) throws IOException {
    delete(StringUtils.bytes2String(category), key);
  }

  @Override
  public void deleteRange(String category, byte[] beginKey, byte[] endKey)
      throws IOException {
    try {
      db.deleteRange(getColumnFamilyHandle(category), beginKey, endKey);
    } catch (RocksDBException e) {
      throw toIOException("Failed to deleteRange of category:" + category, e);
    }
  }

  @Override
  public void deleteRange(byte[] category, byte[] beginKey, byte[] endKey)
      throws IOException {
    deleteRange(StringUtils.bytes2String(category), beginKey, endKey);
  }

//  @Override
//  public List<Map.Entry<byte[], byte[]>> getRangeKVs(byte[] startKey,
//      int count, MetadataKeyFilters.MetadataKeyFilter... filters)
//      throws IOException, IllegalArgumentException {
//    return getRangeKVs(getDefaultColumnFamily(),
//        startKey, count, false, filters);
//  }

  @Override
  public List<Map.Entry<byte[], byte[]>> getRangeKVs(
      String category, byte[] startKey, int count,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(category, startKey, count, false, filters);
  }

  @Override
  public List<Map.Entry<byte[], byte[]>> getRangeKVs(
      byte[] category, byte[] startKey, int count,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(StringUtils.bytes2String(category),
        startKey, count, filters);
  }

//  @Override
//  public List<Map.Entry<byte[], byte[]>> getSequentialRangeKVs(byte[] startKey,
//      int count, MetadataKeyFilters.MetadataKeyFilter... filters)
//      throws IOException, IllegalArgumentException {
//    return getRangeKVs(getDefaultColumnFamily(),
//        startKey, count, true, filters);
//  }

  @Override
  public List<Map.Entry<byte[], byte[]>> getSequentialRangeKVs(
      String category, byte[] startKey, int count,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(category,
        startKey, count, true, filters);
  }

  @Override
  public List<Map.Entry<byte[], byte[]>> getSequentialRangeKVs(
      byte[] category, byte[] startKey, int count,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getSequentialRangeKVs(StringUtils.bytes2String(category),
        startKey, count, filters);
  }

  private List<Map.Entry<byte[], byte[]>> getRangeKVs(
      String category, byte[] startKey,
      int count, boolean sequential,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    List<Map.Entry<byte[], byte[]>> result = new ArrayList<>();
    long start = System.currentTimeMillis();
    if (count < 0) {
      throw new IllegalArgumentException(
          "Invalid count given " + count + ", count must be greater than 0");
    }
    RocksIterator it = null;
    try {
      it = db.newIterator(getColumnFamilyHandle(category));
      if (startKey == null) {
        it.seekToFirst();
      } else {
        if(get(category, startKey) == null) {
          // Key not found, return empty list
          return result;
        }
        it.seek(startKey);
      }
      while(it.isValid() && result.size() < count) {
        byte[] currentKey = it.key();
        byte[] currentValue = it.value();

        it.prev();
        final byte[] prevKey = it.isValid() ? it.key() : null;

        it.seek(currentKey);
        it.next();
        final byte[] nextKey = it.isValid() ? it.key() : null;

        if (filters == null) {
          result.add(new AbstractMap.SimpleImmutableEntry<>(currentKey,
              currentValue));
        } else {
          if (Arrays.asList(filters).stream()
              .allMatch(entry -> entry.filterKey(prevKey,
                  currentKey, nextKey))) {
            result.add(new AbstractMap.SimpleImmutableEntry<>(currentKey,
                currentValue));
          } else {
            if (result.size() > 0 && sequential) {
              // if the caller asks for a sequential range of results,
              // and we met a dis-match, abort iteration from here.
              // if result is empty, we continue to look for the first match.
              break;
            }
          }
        }
      }
    } finally {
      if (it != null) {
        it.close();
      }
      long end = System.currentTimeMillis();
      long timeConsumed = end - start;
      if (LOG.isDebugEnabled()) {
        if (filters != null) {
          for (MetadataKeyFilters.MetadataKeyFilter filter : filters) {
            int scanned = filter.getKeysScannedNum();
            int hinted = filter.getKeysHintedNum();
            if (scanned > 0 || hinted > 0) {
              LOG.debug(
                  "getRangeKVs ({}) numOfKeysScanned={}, numOfKeysHinted={}",
                  filter.getClass().getSimpleName(), filter.getKeysScannedNum(),
                  filter.getKeysHintedNum());
            }
          }
        }
        LOG.debug("Time consumed for getRangeKVs() is {}ms,"
            + " result length is {}.", timeConsumed, result.size());
      }
    }
    return result;
  }

  @Override
  public void writeBatch(BatchOperation operation)
      throws IOException {
    List<BatchOperation.SingleOperation> operations =
        operation.getOperations();
    if (!operations.isEmpty()) {
      try (WriteBatch writeBatch = new WriteBatch()) {
        for (BatchOperation.SingleOperation opt : operations) {
          switch (opt.getOpt()) {
          case DELETE:
            writeBatch.delete(
                getColumnFamilyHandle(opt.getCategory()), opt.getKey());
            break;
          case PUT:
            writeBatch.put(getColumnFamilyHandle(opt.getCategory()),
                opt.getKey(), opt.getValue());
            break;
          default:
            throw new IllegalArgumentException("Invalid operation "
                + opt.getOpt());
          }
        }
        db.write(writeOptions, writeBatch);
      } catch (RocksDBException e) {
        throw toIOException("Batch write operation failed", e);
      }
    }
  }

//  @Override
//  public void compactRange() throws IOException {
//    compactRange(getDefaultColumnFamily());
//  }

  @Override
  public void compactRange(String category) throws IOException {
    if (db != null) {
      try {
        db.compactRange(getColumnFamilyHandle(category));
      } catch (RocksDBException e) {
        throw toIOException("Failed to compact db", e);
      }
    }
  }

  @Override
  public void compactRange(byte[] category) throws IOException {
    compactRange(StringUtils.bytes2String(category));
  }

  @Override
  public void flushDB(boolean sync) throws IOException {
    if (db != null) {
      try {
        // for RocksDB it is sufficient to flush the WAL as entire db can
        // be reconstructed using it.
        db.flushWal(sync);
      } catch (RocksDBException e) {
        throw toIOException("Failed to flush db", e);
      }
    }
  }

//  @Override
//  public void flush() throws IOException {
//    flush(getDefaultColumnFamily());
//  }

  @Override
  public void flush(String category) throws IOException {
    try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
      db.flush(options, getColumnFamilyHandle(category));
    } catch (RocksDBException e) {
      throw toIOException("Failed to flush column:" + category, e);
    }
  }

  @Override
  public void flush(byte[] category) throws IOException {
    flush(StringUtils.bytes2String(category));
  }

  private void deleteQuietly(File fileOrDir) {
    if (fileOrDir != null && fileOrDir.exists()) {
      try {
        FileUtils.forceDelete(fileOrDir);
      } catch (IOException e) {
        LOG.warn("Failed to delete dir {}", fileOrDir.getAbsolutePath(), e);
      }
    }
  }

  @Override
  public void destroy() throws IOException {
    // Make sure db is closed.
    close();

    // There is no destroydb java API available,
    // equivalently we can delete all db directories.
    deleteQuietly(dbLocation);
    deleteQuietly(new File(dbOptions.dbLogDir()));
    deleteQuietly(new File(dbOptions.walDir()));
    List<DbPath> dbPaths = dbOptions.dbPaths();
    if (dbPaths != null) {
      dbPaths.forEach(dbPath -> {
        deleteQuietly(new File(dbPath.toString()));
      });
    }
  }

//  @Override
//  public ImmutablePair<byte[], byte[]> peekAround(int offset,
//      byte[] from) throws IOException, IllegalArgumentException {
//    return peekAround(getDefaultColumnFamily(), offset, from);
//  }

  @Override
  public ImmutablePair<byte[], byte[]> peekAround(String category, int offset,
      byte[] from) throws IOException, IllegalArgumentException {
    RocksIterator it = null;
    try {
      it = db.newIterator(getColumnFamilyHandle(category));
      if (from == null) {
        it.seekToFirst();
      } else {
        it.seek(from);
      }
      if (!it.isValid()) {
        return null;
      }

      switch (offset) {
      case 0:
        break;
      case 1:
        it.next();
        break;
      case -1:
        it.prev();
        break;
      default:
        throw new IllegalArgumentException(
            "Position can only be -1, 0 " + "or 1, but found " + offset);
      }
      return it.isValid() ? new ImmutablePair<>(it.key(), it.value()) : null;
    } finally {
      if (it != null) {
        it.close();
      }
    }
  }

  @Override
  public ImmutablePair<byte[], byte[]> peekAround(byte[] category, int offset,
      byte[] from) throws IOException, IllegalArgumentException {
    return peekAround(StringUtils.bytes2String(category),
        offset, from);
  }

//  @Override
//  public void iterate(byte[] from, EntryConsumer consumer)
//      throws IOException {
//    iterate(getDefaultColumnFamily(), from, consumer);
//  }

  @Override
  public void iterate(String category, byte[] from, EntryConsumer consumer)
      throws IOException {
    RocksIterator it = null;
    try {
      it = db.newIterator(getColumnFamilyHandle(category));
      if (from != null) {
        it.seek(from);
      } else {
        it.seekToFirst();
      }
      while (it.isValid()) {
        if (!consumer.consume(it.key(), it.value())) {
          break;
        }
        it.next();
      }
    } finally {
      if (it != null) {
        it.close();
      }
    }
  }

  @Override
  public void iterate(byte[] category, byte[] from, EntryConsumer consumer)
      throws IOException {
    iterate(StringUtils.bytes2String(category), from, consumer);
  }

  @Override
  public void close() throws IOException {
    if (statMBeanName != null) {
      MBeans.unregister(statMBeanName);
      statMBeanName = null;
    }
    if (db != null) {
      db.close();
      db = null;
    }

  }

  @VisibleForTesting
  protected ObjectName getStatMBeanName() {
    return statMBeanName;
  }

//  @Override
//  public MetaStoreIterator<KeyValue> iterator() throws IOException {
//    return iterator(getDefaultColumnFamily());
//  }

  @Override
  public MetaStoreIterator<KeyValue> iterator(String category) throws IOException {
    return new RocksDBStoreIterator(db.newIterator(getColumnFamilyHandle(category)));
  }

  @Override
  public MetaStoreIterator<KeyValue> iterator(byte[] category) throws IOException {
    return iterator(StringUtils.bytes2String(category));
  }
}

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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.MetadataKeyFilter;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

/**
 * LevelDB interface.
 */
@Deprecated
public class LevelDBStore implements MetadataStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(LevelDBStore.class);

  private DB db;
  private final File dbFile;
  private final Options dbOptions;
  private final WriteOptions writeOptions;

  public LevelDBStore(File dbPath, boolean createIfMissing)
      throws IOException {
    dbOptions = new Options();
    dbOptions.createIfMissing(createIfMissing);
    this.dbFile = dbPath;
    this.writeOptions = new WriteOptions().sync(true);
    openDB(dbPath, dbOptions);
  }

  /**
   * Opens a DB file.
   *
   * @param dbPath          - DB File path
   * @throws IOException
   */
  public LevelDBStore(File dbPath, Options options)
      throws IOException {
    dbOptions = options;
    this.dbFile = dbPath;
    this.writeOptions = new WriteOptions().sync(true);
    openDB(dbPath, dbOptions);
  }

  private void openDB(File dbPath, Options options) throws IOException {
    if (dbPath.getParentFile().mkdirs()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Db path {} created.", dbPath.getParentFile());
      }
    }
    db = JniDBFactory.factory.open(dbPath, options);
    if (LOG.isDebugEnabled()) {
      LOG.debug("LevelDB successfully opened");
      LOG.debug("[Option] cacheSize = {}", options.cacheSize());
      LOG.debug("[Option] createIfMissing = {}", options.createIfMissing());
      LOG.debug("[Option] blockSize = {}", options.blockSize());
      LOG.debug("[Option] compressionType= {}", options.compressionType());
      LOG.debug("[Option] maxOpenFiles= {}", options.maxOpenFiles());
      LOG.debug("[Option] writeBufferSize= {}", options.writeBufferSize());
    }
  }

  /**
   * Puts a Key into file.
   *
   * @param key   - key
   * @param value - value
   */
//  @Override
//  public void put(byte[] key, byte[] value) {
//    db.put(key, value, writeOptions);
//  }

  public void put(String category, byte[] key, byte[] value)
      throws IOException {
    db.put(key, value, writeOptions);
//    throw new IOException(
//        "put with category parameter not supported in LevelDBStore");
  }

  @Override
  public void put(byte[] category, byte[] key, byte[] value)
      throws IOException {
    put(StringUtils.bytes2String(category), key, value);
  }

  /**
   * Get Key.
   *
   * @param key key
   * @return value
   */
//  @Override
//  public byte[] get(byte[] key) {
//    return db.get(key);
//  }

  @Override
  public byte[] get(String category, byte[] key) throws IOException {
    return db.get(key);
//    return new byte[0];
  }

  @Override
  public byte[] get(byte[] category, byte[] key) throws IOException {
    return get(StringUtils.bytes2String(category), key);
  }

  /**
   * Delete Key.
   *
   * @param key - Key
   */
//  @Override
//  public void delete(byte[] key) {
//    db.delete(key);
//  }

  @Override
  public void delete(String category, byte[] key) throws IOException {
    db.delete(key);
  }

  @Override
  public void delete(byte[] category, byte[] key) throws IOException {
    delete(StringUtils.bytes2String(category), key);
  }

  @Override
  public void deleteRange(String category, byte[] beginKey, byte[] endKey)
      throws IOException {
  }

  @Override
  public void deleteRange(byte[] category, byte[] beginKey, byte[] endKey)
      throws IOException {
  }

  /**
   * Closes the DB.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (db != null){
      db.close();
    }
  }

  /**
   * Returns true if the DB is empty.
   *
   * @return boolean
   * @throws IOException
   */
//  @Override
//  public boolean isEmpty() throws IOException {
//    try (DBIterator iter = db.iterator()) {
//      iter.seekToFirst();
//      boolean hasNext = !iter.hasNext();
//      return hasNext;
//    }
//  }

  @Override
  public boolean isEmpty(String category) throws IOException {
    try (DBIterator iter = db.iterator()) {
      iter.seekToFirst();
      boolean hasNext = !iter.hasNext();
      return hasNext;
    }
//    throw new IOException(
//        "isEmpty with category parameter not supported in LevelDBStore");
  }

  @Override
  public boolean isEmpty(byte[] category) throws IOException {
    return isEmpty(StringUtils.bytes2String(category));
  }

  /**
   * Returns the actual levelDB object.
   * @return DB handle.
   */
  public DB getDB() {
    return db;
  }

  /**
   * Returns an iterator on all the key-value pairs in the DB.
   * @return an iterator on DB entries.
   */
  public DBIterator getIterator() {
    return db.iterator();
  }


  @Override
  public void destroy() throws IOException {
    close();
    JniDBFactory.factory.destroy(dbFile, dbOptions);
  }

//  @Override
//  public ImmutablePair<byte[], byte[]> peekAround(int offset,
//      byte[] from) throws IOException, IllegalArgumentException {
//    try (DBIterator it = db.iterator()) {
//      if (from == null) {
//        it.seekToFirst();
//      } else {
//        it.seek(from);
//      }
//      if (!it.hasNext()) {
//        return null;
//      }
//      switch (offset) {
//      case 0:
//        Entry<byte[], byte[]> current = it.next();
//        return new ImmutablePair<>(current.getKey(), current.getValue());
//      case 1:
//        if (it.next() != null && it.hasNext()) {
//          Entry<byte[], byte[]> next = it.peekNext();
//          return new ImmutablePair<>(next.getKey(), next.getValue());
//        }
//        break;
//      case -1:
//        if (it.hasPrev()) {
//          Entry<byte[], byte[]> prev = it.peekPrev();
//          return new ImmutablePair<>(prev.getKey(), prev.getValue());
//        }
//        break;
//      default:
//        throw new IllegalArgumentException(
//            "Position can only be -1, 0 " + "or 1, but found " + offset);
//      }
//    }
//    return null;
//  }

  @Override
  public ImmutablePair<byte[], byte[]> peekAround(String category, int offset,
      byte[] from) throws IOException, IllegalArgumentException {
    try (DBIterator it = db.iterator()) {
      if (from == null) {
        it.seekToFirst();
      } else {
        it.seek(from);
      }
      if (!it.hasNext()) {
        return null;
      }
      switch (offset) {
      case 0:
        Entry<byte[], byte[]> current = it.next();
        return new ImmutablePair<>(current.getKey(), current.getValue());
      case 1:
        if (it.next() != null && it.hasNext()) {
          Entry<byte[], byte[]> next = it.peekNext();
          return new ImmutablePair<>(next.getKey(), next.getValue());
        }
        break;
      case -1:
        if (it.hasPrev()) {
          Entry<byte[], byte[]> prev = it.peekPrev();
          return new ImmutablePair<>(prev.getKey(), prev.getValue());
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Position can only be -1, 0 " + "or 1, but found " + offset);
      }
    }
    return null;
//    throw new IOException(
//        "peekAround with category parameter not supported in LevelDBStore");
  }

  @Override
  public ImmutablePair<byte[], byte[]> peekAround(byte[] category, int offset,
      byte[] from) throws IOException, IllegalArgumentException {
    return peekAround(StringUtils.bytes2String(category), offset, from);
  }

//  @Override
//  public void iterate(byte[] from, EntryConsumer consumer)
//      throws IOException {
//    try (DBIterator iter = db.iterator()) {
//      if (from != null) {
//        iter.seek(from);
//      } else {
//        iter.seekToFirst();
//      }
//      while (iter.hasNext()) {
//        Entry<byte[], byte[]> current = iter.next();
//        if (!consumer.consume(current.getKey(),
//            current.getValue())) {
//          break;
//        }
//      }
//    }
//  }

  @Override
  public void iterate(String category, byte[] from, EntryConsumer consumer)
      throws IOException {
    try (DBIterator iter = db.iterator()) {
      if (from != null) {
        iter.seek(from);
      } else {
        iter.seekToFirst();
      }
      while (iter.hasNext()) {
        Entry<byte[], byte[]> current = iter.next();
        if (!consumer.consume(current.getKey(),
            current.getValue())) {
          break;
        }
      }
    }
//    throw new IOException(
//        "iterate with category parameter not supported in LevelDBStore");
  }

  @Override
  public void iterate(byte[] category, byte[] from, EntryConsumer consumer)
      throws IOException {
    iterate(StringUtils.bytes2String(category), from, consumer);
  }

  /**
   * Compacts the DB by removing deleted keys etc.
   * @throws IOException if there is an error.
   */
//  @Override
//  public void compactRange() throws IOException {
//    if(db != null) {
//      // From LevelDB docs : begin == null and end == null means the whole DB.
//      db.compactRange(null, null);
//    }
//  }

  @Override
  public void compactRange(String category) throws IOException {
    if(db != null) {
      // From LevelDB docs : begin == null and end == null means the whole DB.
      db.compactRange(null, null);
    }
//    throw new IOException(
//        "compactRange with category parameter not supported in LevelDBStore");
  }

  @Override
  public void compactRange(byte[] category) throws IOException {
    compactRange(StringUtils.bytes2String(category));
  }

  @Override
  public void flushDB(boolean sync) {
    // TODO: Implement flush for level db
    // do nothing
  }

//  @Override
//  public void flush() throws IOException {
//
//  }

  @Override
  public void flush(String category) throws IOException {
//    throw new IOException(
//        "flush with category parameter not supported in LevelDBStore");
  }

  @Override
  public void flush(byte[] category) throws IOException {

  }

  @Override
  public void writeBatch(BatchOperation operation) throws IOException {
    List<BatchOperation.SingleOperation> operations =
        operation.getOperations();
    if (!operations.isEmpty()) {
      try (WriteBatch writeBatch = db.createWriteBatch()) {
        for (BatchOperation.SingleOperation opt : operations) {
          switch (opt.getOpt()) {
          case DELETE:
            writeBatch.delete(opt.getKey());
            break;
          case PUT:
            writeBatch.put(opt.getKey(), opt.getValue());
            break;
          default:
            throw new IllegalArgumentException("Invalid operation "
                + opt.getOpt());
          }
        }
        db.write(writeBatch);
      }
    }
  }

//  @Override
//  public List<Map.Entry<byte[], byte[]>> getRangeKVs(byte[] startKey,
//      int count, MetadataKeyFilters.MetadataKeyFilter... filters)
//      throws IOException, IllegalArgumentException {
//    return getRangeKVs(startKey, count, false, filters);
//  }

  @Override
  public List<Entry<byte[], byte[]>> getRangeKVs(String category,
      byte[] startKey, int count, MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(startKey, count, false, filters);
//    throw new IOException(
//        "getRangeKVs with category parameter not supported in LevelDBStore");
  }

  @Override
  public List<Entry<byte[], byte[]>> getRangeKVs(byte[] category,
      byte[] startKey, int count, MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(StringUtils.bytes2String(category),
        startKey, count, filters);
  }

//  @Override
//  public List<Map.Entry<byte[], byte[]>> getSequentialRangeKVs(byte[] startKey,
//      int count, MetadataKeyFilters.MetadataKeyFilter... filters)
//      throws IOException, IllegalArgumentException {
//    return getRangeKVs(startKey, count, true, filters);
//  }

  @Override
  public List<Entry<byte[], byte[]>> getSequentialRangeKVs(String category,
      byte[] startKey, int count, MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(startKey, count, true, filters);
//    throw new IOException("getSequentialRangeKVs with category parameter not" +
//        "supported in LevelDBStore");
  }

  @Override
  public List<Entry<byte[], byte[]>> getSequentialRangeKVs(byte[] category,
      byte[] startKey, int count, MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getSequentialRangeKVs(StringUtils.bytes2String(category),
        startKey, count, filters);
//    throw new IOException("getSequentialRangeKVs with category parameter not" +
//        "supported in LevelDBStore");
  }

  /**
   * Returns a certain range of key value pairs as a list based on a
   * startKey or count. Further a {@link MetadataKeyFilter} can be added to
   * filter keys if necessary. To prevent race conditions while listing
   * entries, this implementation takes a snapshot and lists the entries from
   * the snapshot. This may, on the other hand, cause the range result slight
   * different with actual data if data is updating concurrently.
   * <p>
   * If the startKey is specified and found in levelDB, this key and the keys
   * after this key will be included in the result. If the startKey is null
   * all entries will be included as long as other conditions are satisfied.
   * If the given startKey doesn't exist, an empty list will be returned.
   * <p>
   * The count argument is to limit number of total entries to return,
   * the value for count must be an integer greater than 0.
   * <p>
   * This method allows to specify one or more {@link MetadataKeyFilter}
   * to filter keys by certain condition. Once given, only the entries
   * whose key passes all the filters will be included in the result.
   *
   * @param startKey a start key.
   * @param count max number of entries to return.
   * @param filters customized one or more {@link MetadataKeyFilter}.
   * @return a list of entries found in the database or an empty list if the
   * startKey is invalid.
   * @throws IOException if there are I/O errors.
   * @throws IllegalArgumentException if count is less than 0.
   */
  private List<Entry<byte[], byte[]>> getRangeKVs(byte[] startKey,
      int count, boolean sequential, MetadataKeyFilter... filters)
      throws IOException {
    List<Entry<byte[], byte[]>> result = new ArrayList<>();
    long start = System.currentTimeMillis();
    if (count < 0) {
      throw new IllegalArgumentException(
          "Invalid count given " + count + ", count must be greater than 0");
    }
    Snapshot snapShot = null;
    DBIterator dbIter = null;
    try {
      snapShot = db.getSnapshot();
      ReadOptions readOptions = new ReadOptions().snapshot(snapShot);
      dbIter = db.iterator(readOptions);
      if (startKey == null) {
        dbIter.seekToFirst();
      } else {
        if (db.get(startKey) == null) {
          // Key not found, return empty list
          return result;
        }
        dbIter.seek(startKey);
      }
      while (dbIter.hasNext() && result.size() < count) {
        byte[] preKey = dbIter.hasPrev() ? dbIter.peekPrev().getKey() : null;
        byte[] nextKey = dbIter.hasNext() ? dbIter.peekNext().getKey() : null;
        Entry<byte[], byte[]> current = dbIter.next();

        if (filters == null) {
          result.add(current);
        } else {
          if (Arrays.asList(filters).stream().allMatch(
              entry -> entry.filterKey(preKey, current.getKey(), nextKey))) {
            result.add(current);
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
      if (snapShot != null) {
        snapShot.close();
      }
      if (dbIter != null) {
        dbIter.close();
      }
      if (LOG.isDebugEnabled()) {
        if (filters != null) {
          for (MetadataKeyFilters.MetadataKeyFilter filter : filters) {
            int scanned = filter.getKeysScannedNum();
            int hinted = filter.getKeysHintedNum();
            if (scanned > 0 || hinted > 0) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "getRangeKVs ({}) numOfKeysScanned={}, numOfKeysHinted={}",
                    filter.getClass().getSimpleName(),
                    filter.getKeysScannedNum(), filter.getKeysHintedNum());
              }
            }
          }
        }
        long end = System.currentTimeMillis();
        long timeConsumed = end - start;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Time consumed for getRangeKVs() is {}ms,"
              + " result length is {}.", timeConsumed, result.size());
        }
      }
    }
    return result;
  }

//  @Override
//  public MetaStoreIterator<KeyValue> iterator() {
//    return new LevelDBStoreIterator(db.iterator());
//  }

  @Override
  public MetaStoreIterator<KeyValue> iterator(String category)
      throws IOException {
    return new LevelDBStoreIterator(db.iterator());
//    throw new IOException(
//        "iterator with category parameter not supported in LevelDBStore");
  }

  @Override
  public MetaStoreIterator<KeyValue> iterator(byte[] category)
      throws IOException {
    return iterator(StringUtils.bytes2String(category));
//    throw new IOException(
//        "iterator with category parameter not supported in LevelDBStore");
  }
}

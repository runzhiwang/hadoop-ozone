/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.MetadataStore;
import org.apache.hadoop.hdds.utils.MetadataStoreBuilder;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.ratis.util.Preconditions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet.getDatanodeStorageDirs;

public class DBManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(DBManager.class);

  private final ConcurrentHashMap<String, List<ReferenceCountedDB>>
      volumeDBMap = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, RocksDBCount>
      volumeDBCountMap = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ReferenceCountedDB>
      pathDBMap = new ConcurrentHashMap<>();
  private int maxCategoryInDB;
  private int maxContainersInDB;
  private int maxContainerInCategory;
  private ConfigurationSource conf;
  private List<byte[]> categories;

  public DBManager(List<String> volumes, ConfigurationSource conf)
      throws IOException {
    initDBManager(volumes, conf);
  }

  public DBManager(ConfigurationSource conf) throws IOException {
    Collection<String> rawLocations = getDatanodeStorageDirs(conf);
    List<String> volumes = new ArrayList<>();
    for (String location : rawLocations) {
      volumes.add(location + "/" + HddsVolume.HDDS_VOLUME_DIR);
    }
    initDBManager(volumes, conf);
  }

  public void initDBManager(List<String> volumes, ConfigurationSource conf)
      throws IOException {
    for (String volume : volumes) {
      volumeDBMap.put(volume, new CopyOnWriteArrayList<>());
      volumeDBCountMap.put(volume, new RocksDBCount(
          volume +
              "/" + OzoneConsts.ROCKSDB_COUNT_FILE_NAME));
    }

    this.maxContainersInDB = conf.getInt(
        HddsConfigKeys.HDDS_DATANODE_ROCKSDB_CONTAINER_LIMIT,
        HddsConfigKeys.HDDS_DATANODE_ROCKSDB_CONTAINER_LIMIT_DEFAULT);
    this.maxCategoryInDB = conf.getInt(
        HddsConfigKeys.HDDS_DATANODE_ROCKSDB_COLUMNFAMILY_LIMIT,
        HddsConfigKeys.HDDS_DATANODE_ROCKSDB_COLUMNFAMILY_LIMIT_DEFAULT);
    this.maxContainerInCategory = maxContainersInDB / maxCategoryInDB;
    this.categories = getCategories();
    this.conf = conf;

    initDBDir(volumes);
    reloadDB(volumes);
  }


  private void initDBDir(List<String> volumes) {
    for (String volume : volumes) {
      String dbDir = volume + "/" + OzoneConsts.ROCKSDB_DIR;
      File file = new File(dbDir);
      if (!file.exists()) {
        file.mkdir();
      }
    }
  }

  private void reloadDB(List<String> volumes)
      throws IOException {
    for (String volume : volumes) {
      RocksDBCount dbCount = volumeDBCountMap.get(volume);
      int count = dbCount.getRocksDBCount();
      for (int i = 0; i < count; i ++) {
        String dbFileName = volume + "/" + OzoneConsts.ROCKSDB_DIR + "/" + i;
        File dbFile = new File(dbFileName);
        if (!dbFile.exists()) {
          LOG.error("Rocksdb:" + dbFileName + " does not exist");
          continue;
        }

        MetadataStore metadataStore =
            MetadataStoreBuilder.newBuilder()
                .setDbFile(dbFile)
                .setCreateIfMissing(false)
                .setConf(conf)
                .build();
        ReferenceCountedDB db =
            new ReferenceCountedDB(metadataStore, dbFile.getPath());
        volumeDBMap.get(volume).add(db);
        pathDBMap.put(db.getContainerDBPath(), db);
      }
    }
  }

  public synchronized DBCategory allocateDB(String volumePath) throws IOException {
    Preconditions.assertTrue(volumeDBMap.containsKey(volumePath),
        " volumePath:" + volumePath + " does not exist in volumeDBMap");
    List<ReferenceCountedDB> dbs = volumeDBMap.get(volumePath);
    ReferenceCountedDB db = null;
    for (int i = dbs.size() - 1; i >= 0; i --) {
      MetadataStore store = dbs.get(i).getStore();
      byte[] containerCountKey =
          StringUtils.string2Bytes(OzoneConsts.DB_CONTAINER_COUNT);
      try {
        long containerCount = Longs.fromByteArray(
            store.get(RocksDB.DEFAULT_COLUMN_FAMILY, containerCountKey));
        if (containerCount < maxContainersInDB) {
          db = dbs.get(i);
          break;
        }
      } catch (IOException e) {
        LOG.error("Can not get key:{} from DB",
            OzoneConsts.DB_CONTAINER_COUNT, e);
        continue;
      }
    }

    if (db == null) {
      db = createDB(volumePath);
      dbs.add(db);
      pathDBMap.put(db.getContainerDBPath(), db);
    }

    String category = allocateCategory(db);
    incContainerCount(db.getStore(), category);
    return new DBCategory(db.getContainerDBPath(), category);
  }

  public ReferenceCountedDB getDB(String dbPath) throws IOException {
    if (!pathDBMap.containsKey(dbPath)) {
      throw new IOException("RocksDB:" + dbPath + " does not exist");
    }

    return pathDBMap.get(dbPath);
  }

  private ReferenceCountedDB createDB(String volumePath) throws IOException {
    RocksDBCount dbCount = volumeDBCountMap.get(volumePath);
    int count = dbCount.getRocksDBCount();
    String dbFileName =
        volumePath + "/" + OzoneConsts.ROCKSDB_DIR + "/" + (count + 1);
    File dbFile = new File(dbFileName);
    MetadataStore store = MetadataStoreBuilder.newBuilder().setConf(conf)
        .setCreateIfMissing(true).setDbFile(dbFile).build();
    store.createCategories(categories);
    initContainerCount(store);
    dbCount.incDBCountInFile();
    return new ReferenceCountedDB(store, dbFile.getAbsolutePath());
  }

  @VisibleForTesting
  public void clean() throws IOException {
    for (String volume : volumeDBMap.keySet()) {
      List<ReferenceCountedDB> dbs = volumeDBMap.get(volume);
      for (ReferenceCountedDB db : dbs) {
        db.getStore().close();
      }
      FileUtils.deleteDirectory(new File(volume, OzoneConsts.ROCKSDB_DIR));
      FileUtils.deleteQuietly(
          new File(volume, OzoneConsts.ROCKSDB_COUNT_FILE_NAME));
    }
  }

  private List<byte[]> getCategories() {
    List<byte[]> categories = new ArrayList<>();
    for (int i = 0; i < maxCategoryInDB; i ++) {
      byte[] name =
          StringUtils.string2Bytes(OzoneConsts.CATEGORY_NAME_PREFIX + i);
      categories.add(name);
    }
    return categories;
  }

  private void initContainerCount(
      MetadataStore store) throws IOException {
    byte[] countInDBKey =
        StringUtils.string2Bytes(OzoneConsts.DB_CONTAINER_COUNT);
    store.put(RocksDB.DEFAULT_COLUMN_FAMILY, countInDBKey,
        Longs.toByteArray(0));

    byte[] countInCategoryKey =
        StringUtils.string2Bytes(OzoneConsts.CATEGORY_CONTAINER_COUNT);
    for (byte[] category : categories) {
      store.put(category, countInCategoryKey,
          Longs.toByteArray(0));
    }
  }

  private void incContainerCount(MetadataStore store, String category)
      throws IOException {
    byte[] countInCategoryKey =
        StringUtils.string2Bytes(OzoneConsts.CATEGORY_CONTAINER_COUNT);
    long countInCategoryValue = Longs.fromByteArray(
        store.get(category, countInCategoryKey));
    store.put(category, countInCategoryKey,
        Longs.toByteArray(countInCategoryValue + 1));

    byte[] countInDBKey =
        StringUtils.string2Bytes(OzoneConsts.DB_CONTAINER_COUNT);
    long countInDBValue = Longs.fromByteArray(
        store.get(RocksDB.DEFAULT_COLUMN_FAMILY, countInDBKey));
    store.put(RocksDB.DEFAULT_COLUMN_FAMILY, countInDBKey,
        Longs.toByteArray(countInDBValue + 1));
  }

  private String allocateCategory(ReferenceCountedDB db) throws IOException {
    for (byte[] category : categories) {
      byte[] countInCategoryKey =
          StringUtils.string2Bytes(OzoneConsts.CATEGORY_CONTAINER_COUNT);
      try {
        long containerCount = Longs.fromByteArray(
            db.getStore().get(category, countInCategoryKey));
        if (containerCount < maxContainerInCategory) {
          return StringUtils.bytes2String(category);
        }
      } catch (IOException e) {
        LOG.error("Can not get key:{} from category:{}",
            OzoneConsts.DB_CONTAINER_COUNT,
            category, e);
      }
    }
    throw new IOException("Can not allocate category in db:" +
        db.getContainerDBPath());
  }
}

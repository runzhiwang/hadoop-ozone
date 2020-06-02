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

package org.apache.hadoop.ozone.container.common.utils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.umd.cs.findbugs.io.IO;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.MetadataStore;
import org.apache.hadoop.hdds.utils.MetadataStoreBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import com.google.common.base.Preconditions;
import org.apache.commons.collections.MapIterator;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * container cache is a LRUMap that maintains the DB handles.
 */
public final class ContainerCache {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerCache.class);

  private static final Map<String, ReferenceCountedDB[]> volumeRocksdbMap =
      new ConcurrentHashMap<>();
  private final Lock lock = new ReentrantLock();
  private static ContainerCache cache;
  /**
   * Constructs a cache that holds DBHandle references.
   */
  private ContainerCache(Collection<String> disks, int rocksdbNumPerDisk) {
    for (String disk : disks) {
      volumeRocksdbMap.put(disk, new ReferenceCountedDB[rocksdbNumPerDisk]);
    }
  }

  /**
   * Return a singleton instance of {@link ContainerCache}
   * that holds the DB handlers.
   *
   * @param conf - Configuration.
   * @return A instance of {@link ContainerCache}.
   */
  public synchronized static ContainerCache getInstance(
      ConfigurationSource conf) {
    if (cache == null) {
      Collection<String> disks = MutableVolumeSet.getDatanodeStorageDirs(conf);
      int rocksdbNumPerDisk = conf.getInt(
          OzoneConfigKeys.HDDS_DATANODE_DISK_ROCKSDB_LIMIT,
          OzoneConfigKeys.HDDS_DATANODE_DISK_ROCKSDB_LIMIT_DEFAULT);
      cache = new ContainerCache(disks, rocksdbNumPerDisk);
    }
    return cache;
  }

  /**
   * Closes all the db instances and clear them.
   */
  public void shutdownCache() {
    lock.lock();
    try {
      // close each db
      for (String disk : volumeRocksdbMap.keySet()) {
        ReferenceCountedDB[] dbs = volumeRocksdbMap.get(disk);
        if (dbs == null) {
          continue;
        }

        for (ReferenceCountedDB db : dbs) {
          if (db == null) {
            continue;
          }
          Preconditions.checkArgument(db.cleanup(), "refCount:",
              db.getReferenceCount());
        }
      }

      volumeRocksdbMap.clear();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns a DB handle if available, create the handler otherwise.
   *
   * @param containerID - ID of the container.
   * @param containerDBPath - DB path of the container.
   * @return ReferenceCountedDB.
   */
  public ReferenceCountedDB getDB(long containerID, String hddsVolumeDir,
      String containerDBPath) throws IOException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative.");
    lock.lock();
    try {
      ReferenceCountedDB[] dbs = volumeRocksdbMap.get(hddsVolumeDir);
      if (dbs == null) {
        IOException e = new IOException("Can not find hddsVolumeDir:" +
            hddsVolumeDir + " containerID:" + containerID);
        LOG.error(e.getMessage());
        throw e;
      }

      for (ReferenceCountedDB db : dbs) {
        if (db.getContainerDBPath().equals(containerDBPath)) {
          // increment the reference before returning the object
          db.incrementReference();
          return db;
        }
      }

      IOException e = new IOException(
          "Could not find Rocksdb. Container:" + containerID +
              " containerDBPath:" + containerDBPath);
      LOG.error(e.getMessage());
      throw e;

    } catch (Exception e) {
      LOG.error("Error get DB. Container:{} ContainerPath:{}",
          containerID, containerDBPath, e);
      throw new IOException(e);
    } finally {
      lock.unlock();
    }
  }

  public ReferenceCountedDB allocateDB(long containerID, String hddsVolumeDir,
      String containerDBType, ConfigurationSource conf) throws IOException {
    lock.lock();
    try {
      ReferenceCountedDB[] dbs = volumeRocksdbMap.get(hddsVolumeDir);
      if (dbs == null) {
        IOException e = new IOException("Can not find hddsVolumeDir:" +
            hddsVolumeDir + " containerID:" + containerID);
        LOG.error(e.getMessage());
        throw e;
      }

      int index = (int)containerID % dbs.length;
      if (dbs[index] == null) {
        String containerDBPath = hddsVolumeDir + File.separator +
            Storage.RocksDB_DIR + index;
        MetadataStore metadataStore =
            MetadataStoreBuilder.newBuilder()
                .setDbFile(new File(containerDBPath))
                .setCreateIfMissing(false)
                .setConf(conf)
                .setDBType(containerDBType)
                .build();
        dbs[index] = new ReferenceCountedDB(metadataStore, containerDBPath);
      }
      return dbs[index];
    } catch (Exception e) {
      LOG.error("Error allocate DB. Container:{} hddsVolumeDir:{}",
          containerID, hddsVolumeDir, e);
      throw new IOException(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove container metadata from db.
   *
   * @param containerDBPath - path of the container db file.
   */
  public void removeFromDB(long containerID, String hddsVolumeDir,
      String containerDBPath) {
    lock.lock();
    try {
      ReferenceCountedDB[] dbs = volumeRocksdbMap.get(hddsVolumeDir);
      if (dbs == null) {
        LOG.error("Can not find hddsVolumeDir:" + hddsVolumeDir +
            " containerID:" + containerID);
      }

      for (ReferenceCountedDB db : dbs) {
        if (db.getContainerDBPath().equals(containerDBPath)) {
          // decrease the reference
          db.decrementReference();
          db.getStore().dropCategory(String.valueOf(containerID));
        }
      }
    } catch (Exception e) {
      LOG.error("Error remove from DB. Container:{} hddsVolumeDir:{}",
          containerID, hddsVolumeDir, e);
    } finally {
      lock.unlock();
    }
  }
}

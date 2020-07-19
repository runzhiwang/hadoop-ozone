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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.helpers.RocksDBCountFile;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class RocksDBCount {
  private File file;

  public RocksDBCount(String filePath) throws IOException {
    this.file = new File(filePath);

    RocksDBCountFileState state = analyzeRocksDBCountFileState();
    if (state == RocksDBCountFileState.INIT) {
      createDBCountInFile(0);
    } else if (state == RocksDBCountFileState.MISSING) {
      createDBCountInFile(getRocksDBCountFromDir());
    }
  }

  public void createDBCountInFile(int count) throws IOException {
    long timeStamp = System.currentTimeMillis();
    RocksDBCountFile countFile = new RocksDBCountFile(
        count, timeStamp, timeStamp);
    countFile.createRocksDBCountFile(file);
  }

  public void incDBCountInFile() throws IOException {
    Properties props = RocksDBCountFile.readFrom(file);
    if (props.isEmpty()) {
      throw new InconsistentStorageStateException(
          "rocksdb count file " + file + " is missing");
    }

    int rocksdbCount = Integer.parseInt(
        getProperty(props, OzoneConsts.ROCKSDB_COUNT));
    long createTime = Long.parseLong(
        getProperty(props, OzoneConsts.ROCKSDB_COUNT_FILE_CREATE_TIME));
    RocksDBCountFile countFile = new RocksDBCountFile(
        rocksdbCount + 1, createTime, System.currentTimeMillis());

    countFile.createRocksDBCountFile(file);
  }

  public int getRocksDBCount() throws IOException {
    Properties props = RocksDBCountFile.readFrom(file);
    if (props.isEmpty()) {
      throw new InconsistentStorageStateException(
          "rocksdb count file " + file + " is missing");
    }

    return Integer.parseInt(
        getProperty(props, OzoneConsts.ROCKSDB_COUNT));
  }

  private String getProperty(Properties props, String propName)
      throws InconsistentStorageStateException {
    String value = props.getProperty(propName);
    if (StringUtils.isBlank(value)) {
      throw new InconsistentStorageStateException("Invalid " + propName +
          ". File : " + file + " has null or empty " + propName);
    }
    return value;
  }

  private int getRocksDBCountFromDir() {
    File rocksdbDir = new File(
        file.getParentFile().getAbsolutePath() + File.separator + OzoneConsts.ROCKSDB_DIR);
    if (!rocksdbDir.exists()) {
      return 0;
    }

    File[] files = rocksdbDir.listFiles();
    if (files == null) {
      // RocksDB Root exists and is empty.
      return 0;
    }

    return files.length;
  }

  private RocksDBCountFileState analyzeRocksDBCountFileState() {
    long rocksDBCount = getRocksDBCountFromDir();
    if (rocksDBCount == 0) {
      return RocksDBCountFileState.INIT;
    }

    if (!file.exists()) {
      return RocksDBCountFileState.MISSING;
    }

    return RocksDBCountFileState.NORMAL;
  }

  public enum RocksDBCountFileState {
    INIT,
    NORMAL,
    MISSING,
  }
}

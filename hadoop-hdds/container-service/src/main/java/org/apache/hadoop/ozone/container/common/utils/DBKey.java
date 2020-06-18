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

import com.google.common.primitives.Longs;

import java.nio.charset.StandardCharsets;

public class DBKey {
  private final String prefix;
  private final long containerID;
  private final long blockLocalID;
  private int byteArrayLen;

  public DBKey(String prefix, long containerID, long blockLocalID) {
    this.prefix = prefix;
    this.containerID = containerID;
    this.blockLocalID = blockLocalID;

    this.byteArrayLen = 0;
    if (prefix != null && !prefix.isEmpty()) {
      this.byteArrayLen += prefix.length();
    }
    if (containerID != -1) {
      this.byteArrayLen += 8;
    }
    if (blockLocalID != -1) {
      this.byteArrayLen += 8;
    }
  }

  public String getPrefix() {
    return prefix;
  }

  public long getContainerID() {
    return containerID;
  }

  public long getBlockLocalID() {
    return blockLocalID;
  }

  public int getByteArrayLen() {
    return byteArrayLen;
  }

  private int appendLongToByteArray(byte[] array, int start, long num) {
    if (num == -1) {
      return start;
    }

    for (int j = 7; j >= 0; j --) {
      array[start + j] = (byte) (num & 0xffL);
      num = num >> 8;
    }

    return start + 8;
  }

  private int appendStringToByteArray(byte[] array, int start, String str) {
    if (str == null || str.isEmpty()) {
      return start;
    }

    byte[] strByte = str.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(strByte, 0, array, start, strByte.length);

    return start + strByte.length;
  }

  public byte[] getDBByteKey() {
    byte[] byteKey = new byte[byteArrayLen];

    int start = 0;
    start = appendStringToByteArray(byteKey, start, prefix);
    start = appendLongToByteArray(byteKey, start, containerID);
    start = appendLongToByteArray(byteKey, start, blockLocalID);

    return byteKey;
  }

  @Override
  public String toString() {
    return prefix + "#" + containerID + "#" + blockLocalID;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private String prefix;
    private long containerID;
    private long blockLocalID;
    private byte[] bytes;

    private Builder() {
      prefix = null;
      containerID = -1;
      blockLocalID = -1;
      bytes = null;
    }

    public Builder setPrefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder setContainerID(long containerID) {
      this.containerID = containerID;
      return this;
    }

    public Builder setBlockLocalID(long blockLocalID) {
      this.blockLocalID = blockLocalID;
      return this;
    }

    public Builder setBytes(byte[] bytes) {
      this.bytes = new byte[bytes.length];
      System.arraycopy(bytes, 0, this.bytes, 0, bytes.length);
      return this;
    }

    public DBKey build() {
      if (bytes != null) {
        parseBytes();
      }

      return new DBKey(prefix, containerID, blockLocalID);
    }

    private void parseBytes() {
      int start = 0;

      if (prefix != null && !prefix.isEmpty()) {
        String str = getStringFromByteArray(bytes, start, prefix.length());
        start += prefix.length();
        if (!str.equals(prefix)) {
          throw new IllegalArgumentException("prefix :" + prefix +
              " not equal to:" + str + " got from byte array");
        }
      }

      if (start < bytes.length) {
        containerID = getLongFromByteArray(bytes, start);
        start += 8;
      }

      if (start < bytes.length) {
        blockLocalID = getLongFromByteArray(bytes, start);
        start += 8;
      }
    }

    private String getStringFromByteArray(
        byte[] key, int start, int len) {
      return new String(key, start, len, StandardCharsets.UTF_8);
    }

    private long getLongFromByteArray(byte[] key, int start) {
      return Longs.fromBytes(
          key[start], key[start + 1], key[start + 2], key[start + 3],
          key[start + 4], key[start + 5], key[start + 6], key[start + 7]);
    }
  }
}

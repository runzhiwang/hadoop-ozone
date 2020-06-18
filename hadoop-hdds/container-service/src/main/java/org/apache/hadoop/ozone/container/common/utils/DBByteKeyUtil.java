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

public final class DBByteKeyUtil {

  private static int appendLongToByteArray(byte[] array, int start, long num) {
    if (num == -1) {
      return start;
    }

    for (int j = 7; j >= 0; j --) {
      array[start + j] = (byte) (num & 0xffL);
      num = num >> 8;
    }

    return start + 8;
  }

  private static int appendStringToByteArray(byte[] array, int start, String str) {
    if (str == null || str.isEmpty()) {
      return start;
    }

    byte[] strByte = str.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(strByte, 0, array, start, strByte.length);

    return start + strByte.length;
  }

  public static byte[] getDBByteKey(DBKey dbKey) {
    byte[] byteKey = new byte[dbKey.getByteArrayLen()];

    int start = 0;
    start = appendStringToByteArray(byteKey, start, dbKey.getPrefix());
    start = appendLongToByteArray(byteKey, start, dbKey.getContainerID());
    start = appendLongToByteArray(byteKey, start, dbKey.getBlockLocalID());

    return byteKey;
  }

  private static String getStringFromByteArray(
      byte[] key, int start, int len) {
    return new String(key, start, len, StandardCharsets.UTF_8);
  }

  private static long getLongFromByteArray(byte[] key, int start) {
    return Longs.fromBytes(
        key[start], key[start + 1], key[start + 2], key[start + 3],
        key[start + 4], key[start + 5], key[start + 6], key[start + 7]);
  }

  public static DBKey getDBKey(byte[] key, String prefix) {
    int start = 0;

    String str = getStringFromByteArray(key, start, prefix.length());
    start += prefix.length();
    if (!str.equals(prefix)) {
      throw new IllegalArgumentException("prefix :" + prefix +
          " not equal to:" + str + " got from byte array");
    }

    long containerID = -1;
    if (start < key.length) {
      containerID = getLongFromByteArray(key, start);
      start += 8;
    }

    long blockLocalID = -1;
    if (start < key.length) {
      blockLocalID = getLongFromByteArray(key, start);
      start += 8;
    }

    return DBKey.newBuilder().setPrefix(str).setContainerID(containerID)
        .setBlockLocalID(blockLocalID).build();
  }
}

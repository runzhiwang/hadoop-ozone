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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * An utility class to filter levelDB keys.
 */
public final class MetadataKeyFilters {

  private MetadataKeyFilters() {
  }

  /**
   * Interface for levelDB key filters.
   */
  public interface MetadataKeyFilter {
    /**
     * Filter levelDB key with a certain condition.
     *
     * @param preKey     previous key.
     * @param currentKey current key.
     * @param nextKey    next key.
     * @return true if a certain condition satisfied, return false otherwise.
     */
    boolean filterKey(byte[] preKey, byte[] currentKey, byte[] nextKey);

    default int getKeysScannedNum() {
      return 0;
    }

    default int getKeysHintedNum() {
      return 0;
    }
  }

  /**
   * Utility class to filter key by a string prefix. This filter
   * assumes keys can be parsed to a string.
   */
  public static class KeyPrefixFilter implements MetadataKeyFilter {

    private List<byte[]> positivePrefixList = new ArrayList<>();
    private List<byte[]> negativePrefixList = new ArrayList<>();
    private boolean atleastOnePositiveMatch;
    private int keysScanned = 0;
    private int keysHinted = 0;

    public KeyPrefixFilter() {}

    /**
     * KeyPrefixFilter constructor. It is made of positive and negative prefix
     * list. PositivePrefixList is the list of prefixes which are accepted
     * whereas negativePrefixList contains the list of prefixes which are
     * rejected.
     *
     * @param atleastOnePositiveMatch if positive it requires key to be accepted
     *                               by atleast one positive filter.
     */
    public KeyPrefixFilter(boolean atleastOnePositiveMatch) {
      this.atleastOnePositiveMatch = atleastOnePositiveMatch;
    }

    public KeyPrefixFilter addFilter(String keyPrefix) {
      addFilter(StringUtils.string2Bytes(keyPrefix), false);
      return this;
    }

    public KeyPrefixFilter addFilter(byte[] keyPrefix) {
      addFilter(keyPrefix, false);
      return this;
    }

    public KeyPrefixFilter addFilter(String keyPrefix, boolean negative) {
      addFilter(StringUtils.string2Bytes(keyPrefix), negative);
      return this;
    }

    public KeyPrefixFilter addFilter(byte[] keyPrefix, boolean negative) {
      Preconditions.checkArgument(keyPrefix != null && keyPrefix.length != 0,
          "KeyPrefix is null or empty");
      // keyPrefix which needs to be added should not be prefix of any opposing
      // filter already present. If keyPrefix is a negative filter it should not
      // be a prefix of any positive filter. Nor should any opposing filter be
      // a prefix of keyPrefix.
      // For example if b0 is accepted b can not be rejected and
      // if b is accepted b0 can not be rejected. If these scenarios need to be
      // handled we need to add priorities.
      if (negative) {
        Preconditions.checkArgument(positivePrefixList.stream().noneMatch(
            prefix -> prefixMatch(prefix, keyPrefix) ||
                prefixMatch(keyPrefix, prefix)),
            "KeyPrefix: " + StringUtils.bytes2String(keyPrefix) + " already accepted.");
        this.negativePrefixList.add(keyPrefix);
      } else {
        Preconditions.checkArgument(negativePrefixList.stream().noneMatch(
            prefix -> prefixMatch(prefix, keyPrefix) ||
                prefixMatch(keyPrefix, prefix)),
            "KeyPrefix: " + StringUtils.bytes2String(keyPrefix) + " already rejected.");
        this.positivePrefixList.add(keyPrefix);
      }
      return this;
    }

    @Override
    public boolean filterKey(byte[] preKey, byte[] currentKey,
        byte[] nextKey) {
      keysScanned++;
      if (currentKey == null) {
        return false;
      }
      boolean accept;

      // There are no filters present
      if (positivePrefixList.isEmpty() && negativePrefixList.isEmpty()) {
        return true;
      }

      accept = !positivePrefixList.isEmpty() && positivePrefixList.stream()
          .anyMatch(prefix -> {
            return prefixMatch(prefix, currentKey);
          });
      if (accept) {
        keysHinted++;
        return true;
      } else if (atleastOnePositiveMatch) {
        return false;
      }

      accept = !negativePrefixList.isEmpty() && negativePrefixList.stream()
          .allMatch(prefix -> {
            return !prefixMatch(prefix, currentKey);
          });
      if (accept) {
        keysHinted++;
        return true;
      }

      return false;
    }

    @Override
    public int getKeysScannedNum() {
      return keysScanned;
    }

    @Override
    public int getKeysHintedNum() {
      return keysHinted;
    }

    private static boolean prefixMatch(byte[] prefix, byte[] key) {
      Preconditions.checkNotNull(prefix);
      Preconditions.checkNotNull(key);
      if (key.length < prefix.length) {
        return false;
      }
      for (int i = 0; i < prefix.length; i++) {
        if (key[i] != prefix[i]) {
          return false;
        }
      }
      return true;
    }
  }
}

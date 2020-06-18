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

    private Builder() {
      prefix = null;
      containerID = -1;
      blockLocalID = -1;
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

    public DBKey build() {
      return new DBKey(prefix, containerID, blockLocalID);
    }
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.block;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeleteBlocksCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransactionIDs;
import org.apache.hadoop.hdds.scm.ha.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT;

public class DeletedBlockLogStateManagerImpl
    implements DeletedBlockLogStateManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(DeletedBlockLogStateManagerImpl.class);

  private final Table<Long, DeletedBlocksTransaction> deletedTable;
  private final DBTransactionBuffer transactionBuffer;
  private final int maxRetry;

  public DeletedBlockLogStateManagerImpl(
      ConfigurationSource conf,
      Table<Long, DeletedBlocksTransaction> deletedTable,
      DBTransactionBuffer buffer) {
    this.maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY,
        OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT);
    this.deletedTable = deletedTable;
    this.transactionBuffer = buffer;
  }

  public TableIterator<Long, TypedTable.KeyValue<Long,
      DeletedBlocksTransaction>> getReadOnlyIterator() {
    return new TableIterator<Long, TypedTable.KeyValue<Long,
        DeletedBlocksTransaction>>() {
      private TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
          deletedTable.iterator();

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public TypedTable.KeyValue<Long, DeletedBlocksTransaction> next() {
        return iter.next();
      }

      @Override
      public void close() throws IOException {
        iter.close();
      }

      @Override
      public void seekToFirst() {
        iter.seekToFirst();
      }

      @Override
      public void seekToLast() {
        iter.seekToLast();
      }

      @Override
      public TypedTable.KeyValue<Long, DeletedBlocksTransaction> seek(
          Long key) throws IOException {
        return iter.seek(key);
      }

      @Override
      public Long key() throws IOException {
        return iter.key();
      }

      @Override
      public TypedTable.KeyValue<Long, DeletedBlocksTransaction> value() {
        return iter.value();
      }

      @Override
      public void removeFromDB() throws IOException {
        throw new UnsupportedOperationException("read-only");
      }
    };
  }

  @Override
  public void addTransactionsToDB(ArrayList<DeletedBlocksTransaction> txs)
      throws IOException {
    for (DeletedBlocksTransaction tx : txs) {
      deletedTable.putWithBatch(
          transactionBuffer.getCurrentBatchOperation(), tx.getTxID(), tx);
    }
  }

  @Override
  public void removeTransactionsFromDB(ArrayList<Long> txIDs)
      throws IOException {
    for (Long txID : txIDs) {
      deletedTable.deleteWithBatch(
          transactionBuffer.getCurrentBatchOperation(), txID);
    }
  }

  @Override
  public void increaseRetryCountOfTransactionDB(
      ArrayList<Long> txIDs) throws IOException {
    for (Long txID : txIDs) {
      DeletedBlocksTransaction block =
          deletedTable.get(txID);
      if (block == null) {
        if (LOG.isDebugEnabled()) {
          // This can occur due to race condition between retry and old
          // service task where old task removes the transaction and the new
          // task is resending
          LOG.debug("Deleted TXID {} not found.", txID);
        }
        continue;
      }
      DeletedBlocksTransaction.Builder builder = block.toBuilder();
      int currentCount = block.getCount();
      if (currentCount > -1) {
        builder.setCount(++currentCount);
      }
      // if the retry time exceeds the maxRetry value
      // then set the retry value to -1, stop retrying, admins can
      // analyze those blocks and purge them manually by SCMCli.
      if (currentCount > maxRetry) {
        builder.setCount(-1);
      }
      deletedTable.putWithBatch(
          transactionBuffer.getCurrentBatchOperation(), txID, builder.build());
    }
  }



  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for ContainerStateManager.
   */
  public static class Builder {
    private ConfigurationSource conf;
    private SCMRatisServer scmRatisServer;
    private Table<Long, DeletedBlocksTransaction> table;
    private DBTransactionBuffer transactionBuffer;

    public Builder setConfiguration(final ConfigurationSource config) {
      conf = config;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public Builder setDeletedBlocksTable(
        final Table<Long, DeletedBlocksTransaction> deletedBlocksTable) {
      table = deletedBlocksTable;
      return this;
    }

    public Builder setSCMDBTransactionBuffer(DBTransactionBuffer buffer) {
      this.transactionBuffer = buffer;
      return this;
    }

    public DeletedBlockLogStateManager build() {
      Preconditions.checkNotNull(conf);
      Preconditions.checkNotNull(scmRatisServer);
      Preconditions.checkNotNull(table);
      Preconditions.checkNotNull(table);

      final DeletedBlockLogStateManager impl =
          new DeletedBlockLogStateManagerImpl(conf, table, transactionBuffer);

      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.BLOCK,
              impl, scmRatisServer);

      return (DeletedBlockLogStateManager) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{DeletedBlockLogStateManager.class},
          invocationHandler);
    }

  }
}

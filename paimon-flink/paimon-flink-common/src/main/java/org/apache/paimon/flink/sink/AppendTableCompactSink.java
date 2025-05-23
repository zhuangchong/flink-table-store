/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink;

import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

/** Compaction Sink for unaware-bucket table. */
public class AppendTableCompactSink extends FlinkSink<AppendCompactTask> {

    private final FileStoreTable table;

    public AppendTableCompactSink(FileStoreTable table) {
        super(table, true);
        this.table = table;
    }

    public static DataStreamSink<?> sink(
            FileStoreTable table, DataStream<AppendCompactTask> input) {
        return new AppendTableCompactSink(table).sinkFrom(input);
    }

    @Override
    protected OneInputStreamOperatorFactory<AppendCompactTask, Committable>
            createWriteOperatorFactory(StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new AppendOnlySingleTableCompactionWorkerOperator.Factory(table, commitUser);
    }

    @Override
    protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory() {
        return context -> new StoreCommitter(table, table.newCommit(context.commitUser()), context);
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return new NoopCommittableStateManager();
    }
}

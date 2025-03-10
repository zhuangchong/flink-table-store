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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.schema.Schema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** Compared to {@link CdcMultiplexRecord}, this contains schema information. */
public class RichCdcMultiplexRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final String databaseName;
    @Nullable private final String tableName;
    private final Schema schema;
    private final CdcRecord cdcRecord;

    public RichCdcMultiplexRecord(
            @Nullable String databaseName,
            @Nullable String tableName,
            @Nullable Schema schema,
            CdcRecord cdcRecord) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.schema = schema == null ? Schema.newBuilder().build() : schema;
        this.cdcRecord = cdcRecord;
    }

    @Nullable
    public String databaseName() {
        return databaseName;
    }

    @Nullable
    public String tableName() {
        return tableName;
    }

    public Schema schema() {
        return schema;
    }

    public RichCdcRecord toRichCdcRecord() {
        return new RichCdcRecord(cdcRecord, schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, schema, cdcRecord);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RichCdcMultiplexRecord that = (RichCdcMultiplexRecord) o;
        return Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(schema, that.schema)
                && Objects.equals(cdcRecord, that.cdcRecord);
    }

    @Override
    public String toString() {
        return "{"
                + "databaseName="
                + databaseName
                + ", tableName="
                + tableName
                + ", schema="
                + schema
                + ", cdcRecord="
                + cdcRecord
                + '}';
    }
}

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

package org.apache.paimon.flink.action.cdc.postgres;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.cdc.TypeMapping;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.ArrayList;
import java.util.Optional;

/** Factory to create {@link PostgresSyncTableAction}. */
public class PostgresSyncTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "postgres-sync-table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);
        checkRequiredArgument(params, "postgres-conf");
        PostgresSyncTableAction action =
                new PostgresSyncTableAction(
                        tablePath.f0,
                        tablePath.f1,
                        tablePath.f2,
                        optionalConfigMap(params, "catalog-conf"),
                        optionalConfigMap(params, "postgres-conf"));

        action.withTableConfig(optionalConfigMap(params, "table-conf"));

        if (params.has("partition-keys")) {
            action.withPartitionKeys(params.get("partition-keys").split(","));
        }

        if (params.has("primary-keys")) {
            action.withPrimaryKeys(params.get("primary-keys").split(","));
        }

        if (params.has("computed-column")) {
            action.withComputedColumnArgs(
                    new ArrayList<>(params.getMultiParameter("computed-column")));
        }

        if (params.has("metadata-column")) {
            action.withMetadataKeys(new ArrayList<>(params.getMultiParameter("metadata-column")));
        }

        if (params.has("type-mapping")) {
            String[] options = params.get("type-mapping").split(",");
            action.withTypeMapping(TypeMapping.parse(options));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"postgres-sync-table\" creates a streaming job "
                        + "with a Flink PostgreSQL CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  postgres-sync-table --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> "
                        + "[--partition-keys <partition-keys>] "
                        + "[--primary-keys <primary-keys>] "
                        + "[--type-mapping <option1,option2...>] "
                        + "[--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] "
                        + "[--metadata-column <metadata-column>] "
                        + "[--postgres-conf <postgres-cdc-source-conf> [--postgres-conf <postgres-cdc-source-conf> ...]] "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] "
                        + "[--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]");
        System.out.println();

        System.out.println("Partition keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println(
                "If partition key is not defined and the specified Paimon table does not exist, "
                        + "this action will automatically create an unpartitioned Paimon table.");
        System.out.println();

        System.out.println("Primary keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println("Primary keys will be derived from PostgreSQL tables if not specified.");
        System.out.println();

        System.out.println(
                "--type-mapping is used to specify how to map PostgreSQL type to Paimon type. Please see the doc for usage.");
        System.out.println();

        System.out.println("Please see doc for usage of --computed-column.");
        System.out.println();

        System.out.println(
                "--metadata-column is used to specify which metadata columns to include in the output schema of the connector. Please see the doc for usage.");
        System.out.println();

        System.out.println("PostgresSQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password', 'database-name', 'schema-name', 'table-name' and 'slot.name' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  postgres-sync-table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition-keys pt \\\n"
                        + "    --primary-keys pt,uid \\\n"
                        + "    --postgres-conf hostname=127.0.0.1 \\\n"
                        + "    --postgres-conf username=root \\\n"
                        + "    --postgres-conf password=123456 \\\n"
                        + "    --postgres-conf database-name=source_db \\\n"
                        + "    --postgres-conf schema-name=public \\\n"
                        + "    --postgres-conf table-name='source_table' \\\n"
                        + "    --postgres-conf slot.name=flink \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf bucket=4 \\\n"
                        + "    --table-conf changelog-producer=input \\\n"
                        + "    --table-conf sink.parallelism=4");
    }
}

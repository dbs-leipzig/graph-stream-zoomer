/*
 * Copyright © 2021 - 2023 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.dbsleipzig.stream.grouping.model.table;

import edu.dbsleipzig.stream.grouping.impl.functions.utils.PlannerExpressionSeqBuilder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Responsible for creating instances of {@link TableSetFactory}
 */
public class TableSetFactory {

    /**
     * Constructor
     */
    public TableSetFactory() {
    }

    /**
     * Creates a table set from given table.
     *
     * @param graph graph table
     * @return new table set
     */
    public TableSet fromTable(Table graph, StreamTableEnvironment tableEnvironment) {
        TableSet tableSet = new TableSet();
        tableSet.put(TableSet.TABLE_GRAPH, computeNewGraph(graph, tableEnvironment));
        return tableSet;
    }

    /**
     * Creates a table set from given table.
     *
     * @param vertices vertices table
     * @param edges edges table
     * @return new table set
     */
    public TableSet fromTables(Table vertices, Table edges) {
        TableSet tableSet = new TableSet();

        tableSet.put(TableSet.TABLE_VERTICES, vertices);
        tableSet.put(TableSet.TABLE_EDGES, edges);

        return tableSet;
    }


    /**
     * Performs a
     * <p>
     * SELECT edge_id, edge_label, edge_properties,
     * tail_id, source_label, source_properties,
     * head_id, target_label, target_properties
     * FROM graph
     * <p>
     *
     * @param graph graph table
     * @return new graph table
     */
    private Table computeNewGraph(Table graph, StreamTableEnvironment tableEnvironment) {
        return graph.select(new PlannerExpressionSeqBuilder(tableEnvironment)
                .field(TableSet.FIELD_EDGE_ID)
                .field(TableSet.FIELD_EDGE_LABEL)
                .field(TableSet.FIELD_EDGE_PROPERTIES)

                .field(TableSet.FIELD_SOURCE_ID)
                .field(TableSet.FIELD_VERTEX_SOURCE_LABEL)
                .field(TableSet.FIELD_VERTEX_SOURCE_PROPERTIES)

                .field(TableSet.FIELD_TARGET_ID)
                .field(TableSet.FIELD_VERTEX_TARGET_LABEL)
                .field(TableSet.FIELD_VERTEX_TARGET_PROPERTIES)
                .buildString()
        );
    }
}


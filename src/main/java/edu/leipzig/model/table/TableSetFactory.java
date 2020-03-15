package edu.leipzig.model.table;

import edu.leipzig.impl.functions.utils.PlannerExpressionSeqBuilder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.Expression;

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
    public TableSet fromTable(Table graph) {
        TableSet tableSet = new TableSet();
        tableSet.put(TableSet.TABLE_GRAPH, computeNewGraph(graph));
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
    private Table computeNewGraph(Table graph) {
        return graph.select((Expression) new PlannerExpressionSeqBuilder()
                .field(TableSet.FIELD_EDGE_ID)
                .field(TableSet.FIELD_EDGE_LABEL)
                .field(TableSet.FIELD_EDGE_PROPERTIES)

                .field(TableSet.FIELD_TAIL_ID)
                .field(TableSet.FIELD_VERTEX_SOURCE_LABEL)
                .field(TableSet.FIELD_VERTEX_SOURCE_PROPERTIES)

                .field(TableSet.FIELD_HEAD_ID)
                .field(TableSet.FIELD_VERTEX_TARGET_LABEL)
                .field(TableSet.FIELD_VERTEX_TARGET_PROPERTIES)
                .buildSeq()
        );
    }
}


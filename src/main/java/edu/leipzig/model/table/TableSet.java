package edu.leipzig.model.table;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.HashMap;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Basic table set class which is just a wrapper for a map: tableName->{@link Table}
 * <p>
 * The tableName is a name in graph context, like "vertices". It must not be confused with the
 * internal table name in Flink's table environment!
 *
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 *
 * @link GVETableSet
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.gve;
 */

public class TableSet extends HashMap<String, Table> {
    /**
     * Field name of id in vertices table
     */
    public static final String FIELD_VERTEX_ID = "vertex_id";
    /**
     * Field name of label in vertices table
     */
    public static final String FIELD_VERTEX_LABEL = "vertex_label";

    /**
     * Field name of properties in vertices table
     */
    public static final String FIELD_VERTEX_PROPERTIES = "vertex_properties";
    /**
     * Field name of id in edges table
     */
    public static final String FIELD_EDGE_ID = "edge_id";
    /**
     * Field name of source id in edges table
     */
    public static final String FIELD_SOURCE_ID = "source_id";
    /**
     * Field name of head id in edges table
     */
    public static final String FIELD_TARGET_ID = "target_id";
    /**
     * Field name of label in edges table
     */
    public static final String FIELD_EDGE_LABEL = "edge_label";

    /**
     * Field name of properties in edges table
     */
    public static final String FIELD_EDGE_PROPERTIES = "edge_properties";

    /**
     * Field name of source vertex properties in graph table
     */
    public static final String FIELD_VERTEX_SOURCE_PROPERTIES = "source_properties";

    /**
     * Field name of source vertex label in graph table
     */
    public static final String FIELD_VERTEX_SOURCE_LABEL = "source_label";
    /**
     * Field name of target vertex properties in graph table
     */
    public static final String FIELD_VERTEX_TARGET_PROPERTIES = "target_properties";

    /**
     * Field name of target vertex label in graph table
     */
    public static final String FIELD_VERTEX_TARGET_LABEL = "target_label";

    /**
     * Field name of edge timestamp.
     */
    public static final String FIELD_EVENT_TIME = "event_time";

    public static final String FIELD_VERTEX_EVENT_TIME = "vertex_event_time";

    /**
     * Table key of vertices table
     */
    public static final String TABLE_VERTICES = "vertices";
    /**
     * Table key of edges table
     */
    public static final String TABLE_EDGES = "edges";
    /**
     * Table key of graph table
     */
    static final String TABLE_GRAPH = "graph";
    /**
     * Initial table set schema of stream graph layout
     */

    private static final TableSetSchema SCHEMA = new TableSetSchema(
      ImmutableMap.<String, TableSchema>builder()
        .put(TABLE_VERTICES, new TableSchema.Builder()
          .field(FIELD_VERTEX_ID, DataTypes.STRING())
          .field(FIELD_VERTEX_LABEL, DataTypes.STRING())
          .field(FIELD_VERTEX_PROPERTIES, DataTypes.RAW(TypeInformation.of(Properties.class)))
          .build()
        )
        .put(TABLE_EDGES, new TableSchema.Builder()
          .field(FIELD_EDGE_ID, DataTypes.STRING())
          .field(FIELD_SOURCE_ID, DataTypes.STRING())
          .field(FIELD_TARGET_ID, DataTypes.STRING())
          .field(FIELD_EDGE_LABEL, DataTypes.STRING())
          .field(FIELD_EDGE_PROPERTIES, DataTypes.RAW(TypeInformation.of(Properties.class)))
          .field(FIELD_EVENT_TIME, DataTypes.BIGINT(), $(FIELD_EVENT_TIME).proctime().toString())
          .build()
        )
        .put(TABLE_GRAPH, new TableSchema.Builder()
          .field(FIELD_EDGE_ID, DataTypes.STRING())
          .field(FIELD_EDGE_LABEL, DataTypes.STRING())
          .field(FIELD_EDGE_PROPERTIES, DataTypes.RAW(TypeInformation.of(Properties.class)))

          .field(FIELD_SOURCE_ID, DataTypes.STRING())
          .field(FIELD_VERTEX_SOURCE_LABEL, DataTypes.STRING())
          .field(FIELD_VERTEX_SOURCE_PROPERTIES, DataTypes.RAW(TypeInformation.of(Properties.class)))

          .field(FIELD_TARGET_ID, DataTypes.STRING())
          .field(FIELD_VERTEX_TARGET_LABEL, DataTypes.STRING())
          .field(FIELD_VERTEX_TARGET_PROPERTIES, DataTypes.RAW(TypeInformation.of(Properties.class)))
          .build()
        )
        .build());

    /**
     * Constructor
     */
    public TableSet() {
    }

    /**
     * Return vertices table
     *
     * @return vertices table
     */
    public Table getVertices() {
        return get(TABLE_VERTICES);
    }

    /**
     * Returns edges table
     *
     * @return edges table
     */
    public Table getEdges() {
        return get(TABLE_EDGES);
    }

    /**
     * Returns graph table
     *
     * @return graph table
     */
    public Table getGraph() {
        return get(TABLE_GRAPH);
    }


    /**
     * Projects a given table with a super set of edges and vertices fields to those fields
     *
     * @param table table to project
     * @return projected table
     */
    public Table projectToGraph(Table table) {
        return table.select(SCHEMA.buildProjectExpressions(TABLE_GRAPH));
    }

    /**
     * Get project expressions for an edge.
     *
     * @return an array of project expressions
     */
    public static Expression[] getEdgeProjectExpressions() {
        return new Expression[] {
          $(FIELD_EDGE_ID),
          $(FIELD_SOURCE_ID),
          $(FIELD_TARGET_ID),
          $(FIELD_EDGE_LABEL),
          $(FIELD_EDGE_PROPERTIES),
          $(FIELD_EVENT_TIME).rowtime()
        };
    }

    public static Expression[] getEdgeProjectExpressionsWithCastedRowtime() {
        return new Expression[] {
          $(FIELD_EDGE_ID),
          $(FIELD_SOURCE_ID),
          $(FIELD_TARGET_ID),
          $(FIELD_EDGE_LABEL),
          $(FIELD_EDGE_PROPERTIES),
          $(FIELD_EVENT_TIME).cast(DataTypes.TIMESTAMP()).as(FIELD_EVENT_TIME)
        };
    }

    public static Expression[] getVertexProjectExpressions() {
        return new Expression[] {
          $(FIELD_VERTEX_ID),
          $(FIELD_VERTEX_LABEL),
          $(FIELD_VERTEX_PROPERTIES),
          $(FIELD_EVENT_TIME).rowtime()
        };
    }
}

package edu.leipzig.model.streamGraph;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.properties.Properties;

import java.io.Serializable;

/**
 * Stream Object model
 * Tuple6<id :String, timestamp :Long,label :String,properties :Properties,source :StreamVertex,target :StreamVertex>
 */
public class StreamObject extends Tuple6<String, Long, String, Properties, StreamVertex, StreamVertex>
        implements Serializable {
    /**
     * Default constructor is necessary to apply to POJO rules.
     */
    public StreamObject() {
    }

    /**
     * constructor with all fields
     */
    public StreamObject(String id, long timestamp, String label, Properties properties,
                        StreamVertex source, StreamVertex target) {
        this.f0 = id;
        this.f1 = timestamp;
        this.f2 = label;
        this.f3 = properties;
        this.f4 = source;
        this.f5 = target;
    }

    /**
     * Returns the timestamp
     *
     * @return timestamp
     */
    public long getTimestamp() {
        return this.f1;
    }

    /**
     * Returns the source stream vertex
     *
     * @return stream edge (vertex_id, vertex_label, vertex_properties)
     */
    public StreamVertex getSource() {
        return this.f4;
    }

    /**
     * Returns the target stream vertex
     *
     * @return stream edge (vertex_id, vertex_label, vertex_properties)
     */
    public StreamVertex getTarget() {
        return this.f5;
    }

    /**
     * Returns the stream edge
     *
     * @return stream edge (timestamp, edge_id, tail_id, edge_label, edge_properties, head_id)
     */
    public StreamEdge getEdge() {
        return new StreamEdge(this.f0, this.f1, this.f2, this.f3, this.f4.getVertexId(), this.f5.getVertexId());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("E:");
        sb.append(f0).append(",");
        sb.append(f1).append(",");
        sb.append(f2).append(",");
        sb.append(f3);
        sb.append(";V:").append(f4);
        sb.append(";V:").append(f5);
        return sb.toString();
    }
}

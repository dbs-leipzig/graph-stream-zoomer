package edu.leipzig.model.graph;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.properties.Properties;

import java.io.Serializable;

/**
 * Stream Object model
 * Tuple6<id :String, timestamp :Long,label :String,properties :Properties,source :StreamVertex,target :StreamVertex>
 */
public class StreamTriple extends Tuple6<String, Long, String, Properties, StreamVertex, StreamVertex>
  implements Serializable {

    /**
     * Default constructor is necessary to apply to POJO rules.
     */
    public StreamTriple() {
    }

    /**
     * constructor with all fields
     */
    public StreamTriple(String id, long timestamp, String label, Properties properties, StreamVertex source,
      StreamVertex target) {
        this.f0 = id;
        this.f1 = timestamp;
        this.f2 = label;
        this.f3 = properties;
        this.f4 = source;
        this.f5 = target;
    }

    public String getId() {
        return this.f0;
    }

    public void setId(String id) {
        this.f0 = id;
    }

    /**
     * Returns the timestamp
     *
     * @return timestamp
     */
    public long getTimestamp() {
        return this.f1;
    }

    public void setTimestamp(long timestamp) {
        this.f1 = timestamp;
    }

    public String getLabel() {
        return this.f2;
    }

    public void setLabel(String label) {
        this.f2 = label;
    }

    public Properties getProperties() {
        return this.f3;
    }

    public void setProperties(Properties properties) {
        this.f3 = properties;
    }

    /**
     * Returns the source stream vertex
     *
     * @return stream edge (vertex_id, vertex_label, vertex_properties)
     */
    public StreamVertex getSource() {
        return this.f4;
    }

    public void setSource(StreamVertex source) {
        this.f4 = source;
    }

    /**
     * Returns the target stream vertex
     *
     * @return stream edge (vertex_id, vertex_label, vertex_properties)
     */
    public StreamVertex getTarget() {
        return this.f5;
    }

    public void setTarget(StreamVertex target) {
        this.f5 = target;
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
        return f4 + "-[" + f0 + "(t:" + f1 + "):" + f2 + "{" + f3 + "}" +"]->" + f5;
    }
}

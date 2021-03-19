package edu.leipzig.model.graph;

import org.gradoop.common.model.impl.properties.Properties;

import java.io.Serializable;

/**
 * Stream edge model
 * (timestamp, edge_id, tail_id, edge_label, edge_properties, head_id)
 */

public class StreamEdge implements Serializable {
    private String edge_id;
    private String edge_label;
    private Properties edge_properties;
    private String tail_id;
    private String head_id;
    private long timestamp;

    /**
     * Default constructor is necessary to apply to POJO rules.
     */
    public StreamEdge() {
    }

    /**
     * constructor with all fields
     */
    public StreamEdge(String id, Long timestamp, String label, Properties properties, String sourceId, String targetId) {
        this.edge_id = id;
        this.edge_label = label;
        this.edge_properties = properties;
        this.timestamp = timestamp;
        this.head_id = sourceId;
        this.tail_id = targetId;
    }

    /**
     * @return current edge_id
     */
    public String getEdgeId() {
        return edge_id;
    }

    /**
     * @param edge_id edge_id to set
     */
    public void setEdgeId(String edge_id) {
        this.edge_id = edge_id;
    }

    /**
     * @return current edge_label
     */
    public String getEdgeLabel() {
        return edge_label;
    }

    /**
     * @param edge_label edge_label to set
     */
    public void setEdgeLabel(String edge_label) {
        this.edge_label = edge_label;
    }

    /**
     * @return current edge_properties
     */
    public Properties getEdgeProperties() {
        return edge_properties;
    }

    /**
     * @param edge_properties edge_properties to set
     */
    public void setEdgeProperties(Properties edge_properties) {
        this.edge_properties = edge_properties;
    }

    /**
     * @return current tail_id
     */
    public String getTailId() {
        return tail_id;
    }

    /**
     * @param tail_id tail_id to set
     */
    public void setTailId(String tail_id) {
        this.tail_id = tail_id;
    }

    /**
     * @return current head_id
     */
    public String getHeadId() {
        return head_id;
    }

    /**
     * @param head_id head_id to set
     */
    public void setHeadId(String head_id) {
        this.head_id = head_id;
    }

    /**
     * @return current timestamp
     */
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp timestamp to set
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format("%d:(%s)-[%s%s%s{%s}]->(%s)",
                this.timestamp, this.head_id, this.edge_id,
                this.edge_label != null && !this.edge_label.equals("") ? ":" : "",
                this.edge_label, this.edge_properties == null ? "" : this.edge_properties, this.tail_id);
    }
}

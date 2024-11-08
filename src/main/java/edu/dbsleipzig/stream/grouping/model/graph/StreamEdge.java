/*
 * Copyright © 2021 - 2024 Leipzig University (Database Research Group)
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
package edu.dbsleipzig.stream.grouping.model.graph;

import org.apache.flink.table.annotation.DataTypeHint;
import org.gradoop.common.model.impl.properties.Properties;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Stream edge model (edge_id, edge_label, edge_properties, source_id, target_id, timestamp)
 */
public class StreamEdge implements Serializable {
    private String edge_id;
    private String edge_label;
    @DataTypeHint(value = "RAW", bridgedTo = Properties.class)
    private Properties edge_properties;
    private String source_id;
    private String target_id;
    private Timestamp event_time;

    /**
     * Default constructor is necessary to apply to POJO rules.
     */
    public StreamEdge() {
    }

    /**
     * constructor with all fields
     */
    public StreamEdge(String id, Timestamp event_time, String label, Properties properties, String sourceId, String targetId) {
        this.edge_id = id;
        this.event_time = event_time;
        this.edge_label = label;
        this.edge_properties = properties;
        this.target_id = sourceId;
        this.source_id = targetId;
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
     * @return current source_id
     */
    public String getSourceId() {
        return source_id;
    }

    /**
     * @param source_id source_id to set
     */
    public void setSourceId(String source_id) {
        this.source_id = source_id;
    }

    /**
     * @return current head_id
     */
    public String getTargetId() {
        return target_id;
    }

    /**
     * @param target_id head_id to set
     */
    public void setTargetId(String target_id) {
        this.target_id = target_id;
    }

    /**
     * @return current timestamp
     */
    public Timestamp getEventTime() {
        return event_time;
    }

    /**
     * @param eventTime timestamp to set
     */
    public void setEventTime(Timestamp eventTime) {
        this.event_time = eventTime;
    }

    /**
     * Check equality of the edge without id comparison.
     *
     * @param other the other edge to compare
     * @return true, iff the edge label, properties and timestamp are equal
     */
    public boolean equalsWithoutId(StreamEdge other) {
        return this.getEdgeLabel().equals(other.getEdgeLabel())
          && this.getEdgeProperties().equals(other.getEdgeProperties())
          && this.getEventTime() == other.getEventTime();
    }

    @Override
    public String toString() {
        return String.format("(%s)-[t:%s %s%s%s{%s}]->(%s)",
                this.source_id, this.event_time, this.edge_id,
                this.edge_label != null && !this.edge_label.equals("") ? ":" : "",
                this.edge_label, this.edge_properties == null ? "" : this.edge_properties, this.target_id);
    }
}

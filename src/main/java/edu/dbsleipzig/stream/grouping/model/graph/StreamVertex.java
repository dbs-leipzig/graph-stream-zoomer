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
package edu.dbsleipzig.stream.grouping.model.graph;

import org.gradoop.common.model.impl.properties.Properties;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Stream vertex model (vertex_id, vertex_label, vertex_properties, event_time)
 */
public class StreamVertex implements Serializable {
    private String vertex_id;
    private String vertex_label;
    private Properties vertex_properties;
    private Timestamp event_time;

    /**
     * Default constructor is necessary to apply to POJO rules.
     */
    public StreamVertex() {
    }

    /**
     * constructor with all fields
     */
    public StreamVertex(String vertex_id, String vertex_label, Properties vertex_properties,Timestamp event_time) {
        this.vertex_id = vertex_id;
        this.vertex_label = vertex_label;
        this.vertex_properties = vertex_properties;
        this.event_time = event_time;
    }

    /**
     * @return current vertex_id
     */
    public String getVertexId() {
        return vertex_id;
    }

    /**
     * @param vertex_id vertex_id to set
     */
    public void setVertexId(String vertex_id) {
        this.vertex_id = vertex_id;
    }

    /**
     * @return current vertex_label
     */
    public String getVertexLabel() {
        return vertex_label;
    }

    /**
     * @param vertex_label vertex_label to set
     */
    public void setVertexLabel(String vertex_label) {
        this.vertex_label = vertex_label;
    }

    /**
     * @return current vertex_properties
     */
    public Properties getVertexProperties() {
        return vertex_properties;
    }

    /**
     * @param vertex_properties vertex_properties to set
     */
    public void setVertexProperties(Properties vertex_properties) {
        this.vertex_properties = vertex_properties;
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
     * Check equality of the vertex without id comparison.
     *
     * @param other the other vertex to compare
     * @return true, iff the vertex label and properties are equal
     */
    public boolean equalsWithoutId(StreamVertex other) {
        return this.getVertexLabel().equals(other.getVertexLabel())
          && this.getVertexProperties().equals(other.getVertexProperties())
          && this.getEventTime() == other.getEventTime();
    }

    public String toString() {
        return String.format("(%s(t:%s):%s{%s})",
          this.vertex_id, this.event_time, this.vertex_label, this.vertex_properties == null ? "" : this.vertex_properties);
    }
}

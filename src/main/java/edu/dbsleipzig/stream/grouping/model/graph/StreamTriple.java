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

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.properties.Properties;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Stream Object model
 * Tuple6<id :String, timestamp :Long,label :String,properties :Properties,source :StreamVertex,target :StreamVertex>
 */
public class StreamTriple extends Tuple6<String, Timestamp, String, Properties, StreamVertex, StreamVertex>
  implements Serializable {

    /**
     * Default constructor is necessary to apply to POJO rules.
     */
    public StreamTriple() {
    }

    /**
     * constructor with all fields
     */
    public StreamTriple(String id, Timestamp timestamp, String label, Properties properties, StreamVertex source,
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
    public Timestamp getTimestamp() {
        return this.f1;
    }

    public void setTimestamp(Timestamp timestamp) {
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
     * @return stream vertex
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
     * @return stream vertex
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

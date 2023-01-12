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
package edu.dbsleipzig.stream.grouping.application.functions;

import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import java.sql.Timestamp;
import java.time.ZoneId;

/**
 * produce tweet and user as vertex types
 * new tweet and retweet as edge types
 */
public class TwitterMapper implements FlatMapFunction<String, StreamTriple> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<StreamTriple> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }

        if (value.isEmpty()) {
            return;
        }

        final Status status = TwitterObjectFactory.createStatus(value);
        final User user = status.getUser();

        if (user == null) {
            return;
        }

        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        Timestamp timestamp = new Timestamp(jsonNode.get("timestamp_ms").asLong());

        StreamVertex tweetVertex = new StreamVertex();
        Properties tweetProperties = Properties.create();
        // todo: use long or byte as id for vertex
        tweetVertex.setVertexId(String.valueOf(status.getId()));
        tweetVertex.setVertexLabel("tweet");
        tweetVertex.setEventTime(timestamp);
        tweetProperties.set("source", status.getSource());
        tweetProperties.set("lang", status.getLang());
        if (status.getPlace() != null) {
            tweetProperties.set("city", status.getPlace().getName());
            tweetProperties.set("country", status.getPlace().getCountry());
        }
        tweetVertex.setVertexProperties(tweetProperties);

        StreamVertex userVertex = new StreamVertex();
        Properties userProperties = Properties.create();

        userVertex.setVertexId(String.valueOf(user.getId()));
        userVertex.setVertexLabel("user");
        userVertex.setEventTime(timestamp);
        userProperties.set("name", user.getName());
        userProperties.set("screen_name", user.getScreenName());
        userProperties.set("location", user.getLocation());
        userProperties.set("followers_count", user.getFollowersCount());
        userProperties.set("friends_count", user.getFriendsCount());
        userProperties.set("statuses_count", user.getStatusesCount());
        userVertex.setVertexProperties(userProperties);

        Properties edgeProps = Properties.create();
        edgeProps.set("createdAt", status.getCreatedAt().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
        StreamTriple edge = new StreamTriple(
          GradoopId.get().toString(),
          timestamp,
          "createdByUser",
          edgeProps,
          tweetVertex,
          userVertex);

        out.collect(edge);
    }
}

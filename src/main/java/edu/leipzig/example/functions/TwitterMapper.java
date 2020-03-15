package edu.leipzig.example.functions;

import edu.leipzig.model.streamGraph.StreamObject;
import edu.leipzig.model.streamGraph.StreamVertex;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * produce tweet and user as vertex types
 * new tweet and retweet as edge types
 */
public class TwitterMapper implements FlatMapFunction<String, StreamObject> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<StreamObject> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }

        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

        boolean isEnglish = jsonNode.has("lang") && jsonNode.get("lang").asText().equals("en");
        boolean hasText = jsonNode.has("text");
        boolean retweeted = jsonNode.has("retweeted_status") && !jsonNode.get(("retweeted_status")).isNull();
        if (isEnglish && hasText) {
            Long timestamp = jsonNode.get("timestamp_ms").asLong();
            Properties tweetProps = Properties.create();
            tweetProps.set("id", jsonNode.get("id_str").asText());
            tweetProps.set("lang", jsonNode.get("lang").asText());
            StreamVertex tweet = new StreamVertex(jsonNode.get("id_str").asText(), "Tweet", tweetProps);

            JsonNode jsonUser = jsonNode.get("user");
            Properties userProps = Properties.create();
            userProps.set("id", jsonUser.get("id_str").asText());
            StreamVertex user = new StreamVertex(jsonUser.get("id_str").asText(), "User", userProps);

            Properties edgeProps = Properties.create();
            edgeProps.set("timestamp", jsonNode.get("timestamp_ms").asText());
            out.collect(new StreamObject(GradoopId.get().toString(), timestamp, "NewTweet", edgeProps, tweet, user));
            if (retweeted) {
                tweetProps = Properties.create();
                tweetProps.set("id", jsonNode.get("retweeted_status").get("id_str").asText());
                tweetProps.set("lang", jsonNode.get("retweeted_status").get("lang").asText());
                StreamVertex retweet = new StreamVertex(jsonNode.get("retweeted_status").get("id_str").asText(), "Tweet", tweetProps);
                out.collect(new StreamObject(GradoopId.get().toString(), timestamp, "ReTweet", edgeProps, retweet, user));
            }
        }
    }
}

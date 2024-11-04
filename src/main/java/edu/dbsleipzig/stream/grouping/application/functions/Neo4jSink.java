package edu.dbsleipzig.stream.grouping.application.functions;

import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.util.HashMap;
import java.util.Map;


public class Neo4jSink extends RichSinkFunction<StreamTriple> {
  Session session;

  // URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
  final String dbUri = "bolt://localhost";
  final String dbUser = "neo4j";
  final String dbPassword = "password";

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    Driver driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword));
    driver.verifyConnectivity();
    System.out.println("Connection to Neo4j established.");
    this.session = driver.session();

  }

  @Override
  public void close() throws Exception {
    System.out.println("Connection to Neo4j closed.");
    super.close();
  }

  @Override
  public void invoke(StreamTriple streamTriple, Context context) throws Exception {
    super.invoke(streamTriple, context);

    Result result = writeTriple(streamTriple);
    System.out.println("Triple [" + streamTriple.getId() + "] added to database.");
  }

  private Result writeTriple(StreamTriple streamTriple) {

    // Example Triple
    // (a5fe0f3163f3a33ce729b2ca5adea71fbb163934(t:2024-11-03 10:59:59.999)
    // :Station{id=4:91257e73-d4e5-4b16-9aa6-5f9f5f07f8f3:1188:String})
    // -[c394c5b0edda779daec4c002978a911600267b4d(t:2024-11-03 10:59:59.999):
    // TRIP{minDuration=24:Integer,count=1:Long,operation=CREATE:String,maxDuration=24:Integer,avgDuration=24.0:Double}]
    // ->(19350b6e0ba60ca71b92a6929c00860883fbaf5f(t:2024-11-03 10:59:59.999)
    // :Station{id=4:91257e73-d4e5-4b16-9aa6-5f9f5f07f8f3:373:String})

    String query = "MATCH (s1:Station), (s2:Station) WHERE elementId(s1) = $srcId AND elementId(s2) = $trgId " +
      "WITH s1, s2 " +
      "CREATE (s1)-[t:CDC_TRIP]->(s2) " +
      "SET t.operation = $operation, " +
      "t.minDuration = $minDuration, " +
      "t.maxDuration = $maxDuration, " +
      "t.avgDuration = $avgDuration, " +
      "t.count = $count, " +
      "t.createdAt = $time";

    Map<String, Object> params = new HashMap<>();
    params.put("srcId", streamTriple.getSource().getVertexProperties().get("id").getString());
    params.put("trgId", streamTriple.getTarget().getVertexProperties().get("id").getString());
    params.put("count", streamTriple.getProperties().get("count").getLong());
    params.put("minDuration", streamTriple.getProperties().get("minDuration").getInt());
    params.put("maxDuration", streamTriple.getProperties().get("maxDuration").getInt());
    params.put("avgDuration", streamTriple.getProperties().get("avgDuration").getDouble());
    params.put("operation", streamTriple.getProperties().get("operation").getString());
    params.put("time", streamTriple.getTimestamp().toLocalDateTime());

    return this.session.run(query, params);
  }
}

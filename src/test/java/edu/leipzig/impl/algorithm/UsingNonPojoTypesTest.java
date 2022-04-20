package edu.leipzig.impl.algorithm;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class UsingNonPojoTypesTest {
  final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

  User user1, user2, user3;
  Table usersTable;
  DataStream<User> userStream;
  Timestamp t1,t2;

  HashMap<String, Object> properties1, properties2, properties3;

  EnvironmentSettings settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    .build();

  @Before
  public void init() {
    t1 = new Timestamp(1619511661000L);
    t2 = new Timestamp(1619511662000L);

    user1 = new User("JJ", Properties.create(), t1);
    user2 = new User("Dieter", Properties.create(), t2);
    user3 = new User("Heinrich", Properties.create(), t1);

    properties1 = new HashMap<>();
    properties2 = new HashMap<>();
    properties3 = new HashMap<>();

    properties1.put("numberOfFriends", 10);
    properties1.put("passion", "strange birds");
    properties1.put("fear", "strange birds");
    properties2.put("numberOfFriends", 1);
    properties2.put("passion", "BBQ");
    properties2.put("fear", "vegetarians");
    properties3.put("numberOfFriends", 50);
    properties3.put("passion", "beer");
    properties3.put("fear", "liver");

    Properties propertiesUser1 = Properties.createFromMap(properties1);
    Properties propertiesUser2 = Properties.createFromMap(properties2);
    Properties propertiesUser3 = Properties.createFromMap(properties3);

    user1.setUserProperties(propertiesUser1);
    user2.setUserProperties(propertiesUser2);
    user3.setUserProperties(propertiesUser3);

    userStream = env.fromElements(user1, user2, user3);
    usersTable = tableEnv.fromDataStream(userStream, User.getUserSchema());
    System.out.println(usersTable.getResolvedSchema().toPhysicalRowDataType());
  }

  @Test
  public void testNonPojoStream() throws Exception{
    usersTable.select($("f0")).execute().print();
  }


}

class User {
  private String name;
  private Properties userProperties;
  private Timestamp event_time;

  public User(){}

  public User(String name, Properties userProperties, Timestamp event_time) {
    this.name = name;
    this.userProperties = userProperties;
    this.event_time = event_time;
  }


  public Properties getUserProperties() {
    return userProperties;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setUserProperties(Properties userProperties) {
    this.userProperties = userProperties;
  }


  public static org.apache.flink.table.api.Schema getUserSchema() {
    return org.apache.flink.table.api.Schema.newBuilder()
      .column("name", DataTypes.STRING())
      .column("user_properties", DataTypes.RAW(TypeInformation.of(Properties.class)))
      .column("event_time", DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class))
      .watermark("event_time", $("event_time").minus(lit(10).seconds()))
      .build();
  }

}

class Properties {
  private HashMap<String, Value> properties;

  public Properties(){properties = new HashMap<>(10);};

  public Properties(int capacity) {
    properties = new HashMap<>(capacity);
  }

  public static Properties create() {
    return new Properties();
  }

  public void set(String key, Value value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    properties.put(key, value);
  }

  public static Properties createWithCapacity(int capacity) {
    return new Properties(capacity);
  }

  public static Properties createFromMap(HashMap<String, Object> map) {
    Properties properties;
    properties = Properties.createWithCapacity(map.size());
    for (String key : map.keySet()) {
      properties.set(key, Value.create(map.get(key)));
    }
    return properties;
  }
}

class Value {
  private Object value;

  public Value(Object value){
    this. value = value;
  }

  public static Value create(Object value) {
    return new Value(value);
  }
}
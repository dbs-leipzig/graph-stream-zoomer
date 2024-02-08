package edu.dbsleipzig.stream.grouping.application;

import edu.dbsleipzig.stream.grouping.application.functions.RandomStreamTripleGenerator;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.table.TableSet;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.Expressions.*;
import scala.Tuple2;
import scala.math.BigInt;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;

import static edu.dbsleipzig.stream.grouping.model.table.TableSet.*;
import static org.apache.flink.table.api.Expressions.$;

public class MinimalExample {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        // Init the artificial random data stream
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.executeSql(
                "CREATE TABLE Orders (" +
                        "    order_number BIGINT," +
                        "    price        DECIMAL(32,2)," +
                        "    buyer        ROW<first_name STRING, last_name STRING>," +
                        "    order_time   TIMESTAMP(3)," +
                        "    WATERMARK FOR order_time as order_time - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector' = 'datagen'," +
                        "  'rows-per-second' = '100'" +
                        ")"
        );

       Table orders = tableEnvironment.sqlQuery("SELECT * FROM Orders");

       WindowConfig windowConfig = WindowConfig.create();

       Table windowed = orders
               .window(Tumble.over(windowConfig.getWindowExpression()).on($("order_time")).as("order_time_window"))
               .groupBy($("order_time_window"), $("price"), $("buyer"))
               .select($("order_time_window").rowtime().as("order_time_window1"), $("price").as("price"), $("buyer").as("buyer"));

       Table secondWindow = windowed
               .window(Tumble.over(windowConfig.getWindowExpression()).on($("order_time_window1")).as("order_time_window"))
               .groupBy($("order_time_window"), $("price"), $("buyer"))
               .select($("order_time_window").rowtime().as("order_time_window1"), $("price").as("price"), $("buyer").as("buyer"));

       Table thirdWindow = secondWindow
               .window(Tumble.over(windowConfig.getWindowExpression()).on($("order_time_window1")).as("order_time_window"))
               .groupBy($("order_time_window"), $("price"), $("buyer"))
               .select($("order_time_window").rowtime().as("order_time_window1"), $("price").as("price"), $("buyer").as("buyer"));

       thirdWindow.execute().print();
    }

    class Order {

        long order_number;
        double price;
        Tuple2<String, String> buyer;
        Timestamp order_time;

        public Order(long order_number, double price, Tuple2<String, String> buyer, Timestamp order_time) {
            this.order_number = order_number;
            this.price = price;
            this.buyer = buyer;
            this.order_time = order_time;
        }

        public long getOrder_number() {
            return order_number;
        }

        public void setOrder_number(long order_number) {
            this.order_number = order_number;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public Tuple2<String, String> getBuyer() {
            return buyer;
        }

        public void setBuyer(Tuple2<String, String> buyer) {
            this.buyer = buyer;
        }

        public Timestamp getOrder_time() {
            return order_time;
        }

        public void setOrder_time(Timestamp order_time) {
            this.order_time = order_time;
        }
    }
}

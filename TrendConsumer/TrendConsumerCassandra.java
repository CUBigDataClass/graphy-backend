package com.graphy.kafka;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
/*
The following CQL statement creates the necessary table that stores trends from the Kafka Queue with topic name "demo" for now.

CREATE TABLE graphy.trend(
   trend_id UUID PRIMARY KEY,
   trend_name text
   );
*/
import java.util.Arrays;
import java.util.Properties;
public class TrendConsumerCassandra {

    public void consume(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-consumer-group"); // default topic name
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("demo"));
        //Cassandra Code Below
        String serverIP = "127.0.0.1";
        String keyspace = "graphy";

        Cluster cluster = Cluster.builder()
                .addContactPoints(serverIP)
                .build();

        Session session = cluster.connect(keyspace);

//        String cqlStatement = "SELECT * FROM trends";
//        for (Row row : session.execute(cqlStatement)) {
//            System.out.println(row.toString());
//        }
        String cqlStatementC = "INSERT INTO graphy.trend(trend_id,trend_name) "+"VALUES (now(),";

        //End Cassandra

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
//              System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                cqlStatementC=cqlStatementC+record.value()+")";
                session.execute(cqlStatementC);

        }
    }

    public static void main(String[] args) {
        TrendConsumerCassandra obj = new TrendConsumerCassandra();
        obj.consume();
    }
}

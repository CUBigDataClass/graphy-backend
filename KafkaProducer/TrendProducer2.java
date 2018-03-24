package com.graphy.kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class TrendProducer2 {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("zk.connect","localhost:2181");
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","localhost:9092");
        ProducerConfig config = new ProducerConfig(props);
        Producer producer = new Producer(config);

        String msg = "graphy";
        producer.send(new KeyedMessage("demo",msg));
    }

}

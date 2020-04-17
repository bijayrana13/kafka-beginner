package com.hub.bigdata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class kafka_consumer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(kafka_consumer.class);

        Properties prop = new Properties();
        String bootstrap = "localhost:9092";
        String topic = "state";

        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "My-First-Application");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        consumer.subscribe(Arrays.asList(topic));

        while(true) {
        ConsumerRecords<String, String> consumers = consumer.poll(Duration.ofMillis(100));

        for(ConsumerRecord<String, String> consmr : consumers ) {
           logger.info( "consumer_topic => " + consmr.topic() + ", consumer_partition => " + consmr.partition());
           logger.info("consumer_offset => " + consmr.offset() + ", consumer_key => " + consmr.key()   + ", consumer_value => " + consmr.value() + "\n");

        } }
    }
}

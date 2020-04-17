package com.hub.bigdata;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class kafka_producer {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(kafka_producer.class);
        Properties prop = new Properties();

        String bootstarpserver = "localhost:9092";

        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarpserver);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer(prop);


        for(int i =0; i<10; i++)
        {
        ProducerRecord record = new ProducerRecord("country", "my first message" + Integer.toString(i));


        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("meta info. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "TimeStamp: " + recordMetadata.timestamp() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n"
                            ); }
                    else {
                        logger.error("Error message: " , e);
                    }
                }
            }); }

        producer.flush();
        producer.close();
    }
}

package com.hub.bigdata;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class kafka_producer_keys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(kafka_producer_keys.class);
        Properties prop = new Properties();

        String bootstarpserver = "localhost:9092";

        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarpserver);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer(prop);


        for(int i =0; i<10; i++)
        {

            String topic = "state";
            String key = "id_" + Integer.toString(i);
//            Integer partition = 2;
            String value = "my first message_" + Integer.toString(i);

            logger.info("key is  : " + key );

        ProducerRecord record = new ProducerRecord(topic,key, value);


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
            }).get(); }

        producer.flush();
        producer.close();
    }
}

package com.hub.bigdata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.omg.SendingContext.RunTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class kafka_consumer_thread {

    public static void main(String[] args) {
        new kafka_consumer_thread().run();
    }

    public kafka_consumer_thread() {

    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(kafka_consumer_thread.class.getName());

        String bootstrap = "localhost:9092";
        String topic = "state";
        String groupId = "My-Second-Application";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //Create the consumer runnable
        logger.info("Creating the consume thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrap,
                groupId,
                topic,
                latch
        );

        //Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //Add a ShutDown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrap,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            //Create Consumer Configs
            Properties prop = new Properties();
            prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Create consumer
            consumer = new KafkaConsumer<String, String>(prop);
            //Subscribe consumer to topics
            consumer.subscribe(Arrays.asList(topic));
        }


        public void run() {
            //Poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> consumers = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> consmr : consumers) {
                        logger.info("consumer_topic => " + consmr.topic() + ", consumer_partition => " + consmr.partition());
                        logger.info("consumer_offset => " + consmr.offset() + ", consumer_key => " + consmr.key() + ", consumer_value => " + consmr.value());

                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown Signal!");
            } finally {
                consumer.close();
                //Tell our main code that we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            //The wakeup method is special method to interrupt consumer.poll
            //it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
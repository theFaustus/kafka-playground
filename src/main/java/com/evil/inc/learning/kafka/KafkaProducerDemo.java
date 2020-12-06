package com.evil.inc.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaProducerDemo {

    public static final String BOOSTRAP_SERVERS_URL = "127.0.0.1:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVERS_URL);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "testing", "hello world!");

        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                log.info("topic: " + recordMetadata.topic());
                log.info("partition: " + recordMetadata.partition());
                log.info("offset: " + recordMetadata.offset());
                log.info("timestamp: " + recordMetadata.timestamp());
            } else {
                log.error("Oops...", e);
            }
        });

        producer.flush();
        producer.close();

    }
}

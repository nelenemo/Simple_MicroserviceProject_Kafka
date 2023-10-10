package com.consumer.enduser.services;
import com.consumer.enduser.config.AppConstant;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
@Service

public class KafkaConsumerService {
    private final KafkaConsumer<String, String> consumer;

    public KafkaConsumerService() {
        Properties props = new Properties();
        // Set up your Kafka consumer properties
         props.put("bootstrap.servers", "localhost:9092");
         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "your-consumer-group-id");
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(AppConstant.LOCATION_UPDATE_TOPIC));
    }

    public void consumeRecords() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                // process records
                processRecords(records);
            }
        } catch (OffsetOutOfRangeException e) {
            // handle offset out of range exception
            System.out.println("Offset out of range error occurred while consuming messages from Kafka");
        } finally {
            // close the consumer
            consumer.close();
        }
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        // Implement your logic to process the received records here
    }
}

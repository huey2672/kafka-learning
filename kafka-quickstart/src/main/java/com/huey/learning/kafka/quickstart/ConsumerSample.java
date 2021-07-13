package com.huey.learning.kafka.quickstart;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author huey
 */
public class ConsumerSample {

    public static void main(String[] args) {

        //
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "SIMPLE_CONSUMER");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("SimpleTopic"));

        ConsumerRecords<String, String> records = null;
        while (records == null || records.isEmpty()) {
            System.out.println("Waiting for records.");
            records = consumer.poll(Duration.ofSeconds(1));
        }

        for (ConsumerRecord<String, String> record : records) {
            System.out.println(record.value());
        }

        consumer.close();

    }

}

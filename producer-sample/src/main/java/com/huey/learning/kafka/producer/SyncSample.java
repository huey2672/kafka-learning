package com.huey.learning.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huey
 */
public class SyncSample {

    public static void main(String[] args) {

        // 创建生产者
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 构造消息
        ProducerRecord<String, String> record
                = new ProducerRecord<>("SyncSample", "Hello, Kafka!");

        // 发送消息
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // 关闭消费者
        producer.close();

    }

}

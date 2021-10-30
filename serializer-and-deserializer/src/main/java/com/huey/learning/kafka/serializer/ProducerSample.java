package com.huey.learning.kafka.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huey
 */
public class ProducerSample {

    public static void main(String[] args) {

        // 创建生产者
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ObjectSerializer.class.getName());
        Producer<String, Message> producer = new KafkaProducer<>(props);

        // 构造消息
        Message message = new Message("Kafka Learning", "Hello, Kafka");
        ProducerRecord<String, Message> record
                = new ProducerRecord<>("Topic-Serializer-Sample", message);
        // 发送消息
        producer.send(record);

        // 关闭消费者
        producer.close();

    }

}

package com.huey.learning.kafka.serializer;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * @author huey
 */
public class ObjectSerializer implements Serializer<Object> {

    @Override
    public byte[] serialize(String topic, Object data) {
        byte[] bytes;
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(bout)) {
            output.writeObject(data);
            bytes = bout.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException("IOException while writing object data", e);
        }
        return bytes;
    }

}

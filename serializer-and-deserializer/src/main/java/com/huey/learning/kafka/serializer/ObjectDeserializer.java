package com.huey.learning.kafka.serializer;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

/**
 * @author huey
 */
public class ObjectDeserializer implements Deserializer<Object> {

    @Override
    public Object deserialize(String topic, byte[] data) {
        Object object;
        try (ByteArrayInputStream bin = new ByteArrayInputStream(data);
             ObjectInputStream input = new ObjectInputStream(bin)) {
            object = input.readObject();
        }
        catch (Exception e) {
            throw new RuntimeException("IOException while reading object data", e);
        }
        return object;
    }

}

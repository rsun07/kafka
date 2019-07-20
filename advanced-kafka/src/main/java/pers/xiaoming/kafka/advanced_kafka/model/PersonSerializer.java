package pers.xiaoming.kafka.advanced_kafka.model;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class PersonSerializer implements Serializer<Person> {
    @Override
    public byte[] serialize(String topic, Person data) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(data);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}

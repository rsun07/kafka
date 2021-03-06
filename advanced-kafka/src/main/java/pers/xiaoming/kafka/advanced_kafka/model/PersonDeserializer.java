package pers.xiaoming.kafka.advanced_kafka.model;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class PersonDeserializer implements Deserializer<Person> {
    @Override
    public Person deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (Person) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SerializationException(e);
        }
    }
}

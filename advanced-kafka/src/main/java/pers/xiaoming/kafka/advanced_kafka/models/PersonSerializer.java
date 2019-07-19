package pers.xiaoming.kafka.advanced_kafka.models;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class PersonSerializer implements Serializer<Person> {
    @Override
    public byte[] serialize(String topic, Person data) {
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }
}

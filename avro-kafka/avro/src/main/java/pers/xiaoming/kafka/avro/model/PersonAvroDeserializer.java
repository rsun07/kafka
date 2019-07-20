package pers.xiaoming.kafka.avro.model;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;

@Slf4j
public class PersonAvroDeserializer implements Deserializer<Person> {
    @Override
    public Person deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        Person person = new Person();
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
            DatumReader<Person> reader = new SpecificDatumReader<>(person.getSchema());
            BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(bis, null);
            person = reader.read(null, decoder);
        } catch (IOException e) {
            log.error("Failed to deserialize data {}", data);
            throw new SerializationException(e);
        }
        return person;
    }
}

package pers.xiaoming.kafka.avro.models;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class PersonAvroDeserializer implements Deserializer<Person> {
    @Override
    public Person deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        Person person = new Person();
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
            DatumReader<Person> reader = new SpecificDatumReader<>(person.getSchema());
            BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(bis, null);
            person = reader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
        return person;
    }
}

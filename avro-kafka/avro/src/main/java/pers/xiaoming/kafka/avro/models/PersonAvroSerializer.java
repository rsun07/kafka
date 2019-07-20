package pers.xiaoming.kafka.avro.models;


import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PersonAvroSerializer implements Serializer<Person> {
    @Override
    public byte[] serialize(String topic, Person data) {
        if (data == null) {
            return null;
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            DatumWriter<Person> writer = new SpecificDatumWriter<>(data.getSchema());
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);
            writer.write(data, encoder);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
        return new byte[0];
    }
}

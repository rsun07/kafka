package pers.xiaoming.kafka.avro.model;


import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Slf4j
public class PersonAvroSerializer implements Serializer<Person> {
    @Override
    public byte[] serialize(String topic, Person data) {
        if (data == null || data.getId() == null) {
            return null;
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            DatumWriter<Person> writer = new SpecificDatumWriter<>(data.getSchema());
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);
            writer.write(data, encoder);
            return bos.toByteArray();
        } catch (IOException e) {
            log.error("Failed to serialize data {}", data);
            throw new SerializationException(e);
        }
    }
}

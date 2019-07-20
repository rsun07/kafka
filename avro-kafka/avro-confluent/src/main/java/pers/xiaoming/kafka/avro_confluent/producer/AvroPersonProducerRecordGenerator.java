package pers.xiaoming.kafka.avro_confluent.producer;

import javafx.util.Pair;
import pers.xiaoming.kafka.advanced_kafka.producer.ProducerRecordGenerator;
import pers.xiaoming.kafka.avro_confluent.model.Person;

import java.util.concurrent.atomic.AtomicInteger;

public class AvroPersonProducerRecordGenerator implements ProducerRecordGenerator<Integer, Person> {
    private AtomicInteger idSource = new AtomicInteger(0);

    @Override
    public Pair<Integer, Person> nextRecord() {
        int id = idSource.getAndIncrement();
        Person person = new Person(id, "Person_" + id);
        return new Pair<>(id, person);
    }
}

package pers.xiaoming.kafka.advanced_kafka.producer;

import javafx.util.Pair;
import pers.xiaoming.kafka.advanced_kafka.model.Person;

import java.util.concurrent.atomic.AtomicInteger;

public class PersonProducerRecordGenerator implements ProducerRecordGenerator<Integer, Person> {
    private AtomicInteger idSource = new AtomicInteger(0);

    @Override
    public Pair<Integer, Person> nextRecord() {
        int id = idSource.getAndIncrement();
        Person person = new Person(id, "Person_" + id);
        return new Pair<>(id, person);
    }
}

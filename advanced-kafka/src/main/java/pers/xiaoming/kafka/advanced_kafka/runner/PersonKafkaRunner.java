package pers.xiaoming.kafka.advanced_kafka.runner;

import pers.xiaoming.kafka.advanced_kafka.model.Person;
import pers.xiaoming.kafka.advanced_kafka.producer.PersonProducerRecordGenerator;
import pers.xiaoming.kafka.advanced_kafka.producer.ProducerRecordGenerator;

public class PersonKafkaRunner {
    private static final int NUM_OF_PRODUCER = 2;
    private static final int NUM_OF_CONSUMER = 4;

    private static final ProducerRecordGenerator<Integer, Person> RECORD_GENERATOR = new PersonProducerRecordGenerator();

    public static void main(String[] args) {
        GenericRunner<Integer, Person> runner = new GenericRunner<>(NUM_OF_PRODUCER, NUM_OF_CONSUMER, RECORD_GENERATOR);
        runner.start();
    }
}

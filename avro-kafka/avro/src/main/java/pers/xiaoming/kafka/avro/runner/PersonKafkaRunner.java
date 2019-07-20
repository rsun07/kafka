package pers.xiaoming.kafka.avro.runner;

import pers.xiaoming.kafka.advanced_kafka.producer.ProducerRecordGenerator;
import pers.xiaoming.kafka.advanced_kafka.runner.GenericRunner;

import pers.xiaoming.kafka.avro.model.Person;
import pers.xiaoming.kafka.avro.producer.AvroPersonProducerRecordGenerator;

public class PersonKafkaRunner {
    private static final ProducerRecordGenerator<Integer, Person> RECORD_GENERATOR = new AvroPersonProducerRecordGenerator();

    public static void main(String[] args) {
        GenericRunner<Integer, Person> runner = new GenericRunner<>(RECORD_GENERATOR);
        runner.start();
    }
}

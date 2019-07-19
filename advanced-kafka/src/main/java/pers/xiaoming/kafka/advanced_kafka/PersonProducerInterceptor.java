package pers.xiaoming.kafka.advanced_kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pers.xiaoming.kafka.advanced_kafka.models.Person;

import java.util.Map;

@Slf4j
public class PersonProducerInterceptor implements ProducerInterceptor<Integer, Person> {
    @Override
    public ProducerRecord<Integer, Person> onSend(ProducerRecord<Integer, Person> record) {
        log.info("Interceptor: key {}, value {}, for topic {} is been sending to partition {} at {}",
                record.key(), record.value(), record.topic(), record.partition(), record.timestamp());
        return null;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        log.info("Interceptor: message for topic {} has been set to partition {}, offset {} at {} ",
                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}

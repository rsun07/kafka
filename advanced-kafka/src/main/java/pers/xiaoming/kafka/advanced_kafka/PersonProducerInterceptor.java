package pers.xiaoming.kafka.advanced_kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

@Slf4j
public class PersonProducerInterceptor implements ProducerInterceptor<Integer, String> {
    @Override
    public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
        return null;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        log.info("Interceptor: message has been set to partition {}, offset {} at {} ",
                metadata.partition(), metadata.offset(), metadata.timestamp());
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}

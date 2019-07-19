package pers.xiaoming.kafka.basic_kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class Consumer extends Thread implements Closeable {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    public Consumer() throws IOException {

        Properties properties = PropertyUtils.loadProperties("consumer.properties");

        this.topic = properties.getProperty("topic");
        this.consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(this.topic));
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                log.info("Received message: key {}, value {}, at offset {}",
                        record.key(), record.value(), record.offset());
            }
        }
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}

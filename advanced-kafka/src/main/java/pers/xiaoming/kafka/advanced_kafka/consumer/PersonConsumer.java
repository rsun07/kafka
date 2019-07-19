package pers.xiaoming.kafka.advanced_kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import pers.xiaoming.kafka.advanced_kafka.PropertyUtils;
import pers.xiaoming.kafka.advanced_kafka.models.Person;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class PersonConsumer extends Thread implements Closeable {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    public PersonConsumer(String propertyFileName) throws IOException {

        Properties properties = PropertyUtils.loadProperties(propertyFileName);

        this.topic = properties.getProperty("topic");
        this.consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(this.topic), new MyConsumerRebalanceListener(topic));
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

    @RequiredArgsConstructor
    private class MyConsumerRebalanceListener implements ConsumerRebalanceListener {
        private final String topic;

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("Topic {} on partitions {} revoked", topic, partitions.toString());
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("Topic {} on partitions {} assigned", topic, partitions.toString());
        }
    }
}

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PersonConsumer extends Thread implements Closeable {
    private final KafkaConsumer<Integer, Person> consumer;
    private final String topic;
    private final AtomicBoolean shouldRun;
    private CountDownLatch stopLatch;

    public PersonConsumer(String topic, Properties properties) {
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(properties);
        this.shouldRun = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        shouldRun.set(true);
        stopLatch = new CountDownLatch(1);
        consumer.subscribe(Collections.singletonList(this.topic), new MyConsumerRebalanceListener(topic));
        log.info("Subscribe into topic {}, partitions {}", topic, consumer.assignment().toString());

        while (shouldRun.get()) {
            ConsumerRecords<Integer, Person> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, Person> record : records) {
                log.info("Received message: key {}, value {}, at offset {}",
                        record.key(), record.value(), record.offset());
            }
        }

        stopLatch.countDown();
    }

    @Override
    public void close() throws IOException {
        shouldRun.set(false);

        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }

        if (consumer != null) {
            consumer.close();
        }
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

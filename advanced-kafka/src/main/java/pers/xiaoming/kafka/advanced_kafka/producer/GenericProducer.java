package pers.xiaoming.kafka.advanced_kafka.producer;

import javafx.util.Pair;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pers.xiaoming.kafka.advanced_kafka.model.Person;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class GenericProducer<K, V> extends Thread implements AutoCloseable {
    private final KafkaProducer<K, V> producer;
    private final String topic;

    private ProducerRecordGenerator<K, V> dataGenerator;

    public GenericProducer(Properties properties) throws IOException {
        this.topic = properties.getProperty("topic");
        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        while (true) {
            Pair<K, V> record = dataGenerator.nextRecord();
            long startTime = System.currentTimeMillis();
            producer.send(new ProducerRecord<>(topic, record.getKey(), record.getValue()),
                    new MyCallBack(startTime, record.getKey(), record.getValue()));

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.flush();
            producer.close(Duration.ofMillis(10_000));
        }
    }

    @RequiredArgsConstructor
    private class MyCallBack implements Callback {
        private final long startTime;
        private final K key;
        private final V value;

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;

            if (metadata == null && exception != null) {
                log.error("Failed to send message: Message No: {},  {} in {} ms. Exception {}", key, value, elapsedTime, exception);
            }

            if (metadata != null) {
                log.info("Message No: {},  {}, has been set to partition {}, offset {} in {} ms",
                        key, value, metadata.partition(), metadata.offset(), elapsedTime);
            }
        }
    }
}

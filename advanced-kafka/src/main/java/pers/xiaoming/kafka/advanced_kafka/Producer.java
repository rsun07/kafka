package pers.xiaoming.kafka.advanced_kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pers.xiaoming.kafka.advanced_kafka.models.Person;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Producer extends Thread implements AutoCloseable {
    private final KafkaProducer<Integer, Person> producer;
    private final String topic;
    private final AtomicInteger no;

    public Producer(String propertyFileName) throws IOException {
        this.no = new AtomicInteger(0);

        Properties properties = PropertyUtils.loadProperties(propertyFileName);
        this.topic = properties.getProperty("topic");
        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        while (true) {
            int id = no.getAndIncrement();
            Person person = new Person(id, "Person");
            long startTime = System.currentTimeMillis();
            producer.send(new ProducerRecord<>(topic, id, person),
                    new MyCallBack(startTime, id, person));

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

    private class MyCallBack implements Callback {
        private final long startTime;
        private final int key;
        private final Person message;

        MyCallBack(long startTime, int key, Person message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            log.info("Message No: {},  {}, has been set to partition {}, offset {} in {} ms",
                    key, message, metadata.partition(), metadata.offset(), elapsedTime);
        }
    }
}

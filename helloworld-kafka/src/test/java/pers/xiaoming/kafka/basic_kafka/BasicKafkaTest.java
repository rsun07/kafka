package pers.xiaoming.kafka.basic_kafka;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

@Ignore
public class BasicKafkaTest {
    @Test
    public void testAsync() throws IOException, InterruptedException {
        Producer producer = new Producer("producer.properties", true);
        producer.start();

        Consumer consumer = new Consumer("consumer.properties");
        consumer.start();

        producer.join();
        consumer.join();
    }

    @Test
    public void testSync() throws IOException, InterruptedException {
        Producer producer = new Producer("producer.properties", false);
        producer.start();

        Consumer consumer = new Consumer("consumer.properties");
        consumer.start();

        producer.join();
        consumer.join();
    }
}

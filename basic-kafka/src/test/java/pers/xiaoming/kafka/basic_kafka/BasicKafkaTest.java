package pers.xiaoming.kafka.basic_kafka;

import org.junit.Test;

import java.io.IOException;

public class BasicKafkaTest {
    @Test
    public void testAsync() throws IOException {
        Producer producer = new Producer("producer.properties", true);
        producer.start();

        Consumer consumer = new Consumer("consumer.properties");
        consumer.start();
    }
}

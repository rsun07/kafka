package pers.xiaoming.kafka.advanced_kafka;

import org.junit.Test;
import pers.xiaoming.kafka.advanced_kafka.consumer.PersonConsumerManager;
import pers.xiaoming.kafka.advanced_kafka.producer.PersonProducer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AdvancedKafkaTest {
    private static final int numOfProducer = 2;
    private static final int numOfConsumer = 4;

    @Test
    public void test() throws IOException, InterruptedException {

        ExecutorService producerExecutor = Executors.newFixedThreadPool(numOfProducer);
        for (int i = 0; i < numOfProducer; i++) {
            producerExecutor.submit(new PersonProducer("producer.properties"));
        }


        PersonConsumerManager consumerManager = new PersonConsumerManager("consumer.properties", numOfConsumer);
        consumerManager.startAll();

        producerExecutor.awaitTermination(1, TimeUnit.MINUTES);
        consumerManager.stopAll();
    }
}

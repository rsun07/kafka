package pers.xiaoming.kafka.advanced_kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import pers.xiaoming.kafka.advanced_kafka.consumer.PersonConsumerManager;
import pers.xiaoming.kafka.advanced_kafka.producer.PersonProducer;

import java.io.IOException;
import java.util.Properties;
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
            Properties properties = PropertyUtils.loadProperties("producer.properties");
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, String.valueOf(i));
            producerExecutor.submit(new PersonProducer(properties));
        }


        PersonConsumerManager consumerManager = new PersonConsumerManager("consumer.properties", numOfConsumer);
        consumerManager.startAll();

        producerExecutor.awaitTermination(30, TimeUnit.SECONDS);
        consumerManager.stopAll();
    }
}

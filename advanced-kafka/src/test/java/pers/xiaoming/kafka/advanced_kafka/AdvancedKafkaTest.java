package pers.xiaoming.kafka.advanced_kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Ignore;
import org.junit.Test;
import pers.xiaoming.kafka.advanced_kafka.consumer.GenericConsumerManager;
import pers.xiaoming.kafka.advanced_kafka.producer.GenericProducer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Ignore
public class AdvancedKafkaTest {
    private static final int numOfProducer = 2;
    private static final int numOfConsumer = 4;

    @Test
    public void test() throws IOException, InterruptedException {

        ExecutorService producerExecutor = Executors.newFixedThreadPool(numOfProducer);
        for (int i = 0; i < numOfProducer; i++) {
            Properties properties = PropertyUtils.loadProperties("producer.properties");
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, String.valueOf(i));
            producerExecutor.submit(new GenericProducer(properties));
        }


        GenericConsumerManager consumerManager = new GenericConsumerManager("consumer.properties", numOfConsumer);
        consumerManager.startAll();

        producerExecutor.awaitTermination(3, TimeUnit.SECONDS);
        consumerManager.stopAll();
    }
}

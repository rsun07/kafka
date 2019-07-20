package pers.xiaoming.kafka.advanced_kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import pers.xiaoming.kafka.advanced_kafka.PropertyUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class GenericConsumerManager<K, V> {
    private final Properties properties;
    private final String topic;
    private final int numOfPartition;
    private final AtomicInteger clientIdGenerator;

    private Set<GenericConsumer<K, V>> consumers;

    public GenericConsumerManager(String propertyFileName, int numOfPartition) throws IOException {
        this.numOfPartition = numOfPartition;

        this.properties = PropertyUtils.loadProperties(propertyFileName);
        this.topic = properties.getProperty("topic");

        this.clientIdGenerator = new AtomicInteger(0);
    }

    public void startAll() {
        ExecutorService executor = Executors.newFixedThreadPool(numOfPartition);
        consumers = new HashSet<>();

        for (int i = 0; i < numOfPartition; i++) {
            String clientId = String.valueOf(clientIdGenerator.getAndIncrement());
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

            GenericConsumer<K, V> consumer = new GenericConsumer<>(topic, properties);
            executor.execute(consumer);
            consumers.add(consumer);
        }
    }

    public void stopAll() throws IOException {
        for (GenericConsumer consumer : consumers) {
            consumer.close();
        }
    }
}

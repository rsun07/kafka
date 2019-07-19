package pers.xiaoming.kafka.advanced_kafka.consumer;

import pers.xiaoming.kafka.advanced_kafka.PropertyUtils;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PersonConsumerManager {
    private final Properties properties;
    private final String topic;
    private final int numOfPartition;

    public PersonConsumerManager(String propertyFileName, int numOfPartition) throws IOException {
        this.numOfPartition = numOfPartition;

        this.properties = PropertyUtils.loadProperties(propertyFileName);
        this.topic = properties.getProperty("topic");
    }

    public void startAll() {
        ExecutorService executor = Executors.newFixedThreadPool(numOfPartition);

        for (int i = 0; i < numOfPartition; i++) {
            executor.execute(new PersonConsumer(topic, properties));
        }
    }
}

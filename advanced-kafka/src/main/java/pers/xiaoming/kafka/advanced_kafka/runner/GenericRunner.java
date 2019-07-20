package pers.xiaoming.kafka.advanced_kafka.runner;

import org.apache.kafka.clients.producer.ProducerConfig;
import pers.xiaoming.kafka.advanced_kafka.consumer.GenericConsumerManager;
import pers.xiaoming.kafka.advanced_kafka.producer.GenericProducer;
import pers.xiaoming.kafka.advanced_kafka.producer.ProducerRecordGenerator;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GenericRunner<K, V> extends Thread {
    private static final int DEFAULT_NUM_OF_PRODUCER = 1;
    private static final int DEFAULT_NUM_OF_CONSUMER = 1;
    private static final String DEFAULT_PRODUCER_PROPERTY_FILE_NAME = "producer.properties";
    private static final String DEFAULT_CONSUMER_PROPERTY_FILE_NAME = "consumer.properties";
    private static final int DEFAULT_TIMEOUT = 3;
    private static final TimeUnit DEFAULT_TIMEOUT_UNIT = TimeUnit.SECONDS;

    private final int numOfProducer;
    private final int numOfConsumer;
    private final String producerPropertyFileName;
    private final String consumerPropertyFileName;
    private final ProducerRecordGenerator<K, V> recordGenerator;

    private final int timeout;
    private final TimeUnit timeUnit;

    public GenericRunner(ProducerRecordGenerator<K, V> recordGenerator) {
        this(DEFAULT_NUM_OF_PRODUCER, DEFAULT_NUM_OF_CONSUMER,
                DEFAULT_PRODUCER_PROPERTY_FILE_NAME, DEFAULT_CONSUMER_PROPERTY_FILE_NAME,
                recordGenerator, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT_UNIT);
    }

    public GenericRunner(int numOfProducer, int numOfConsumer, ProducerRecordGenerator<K, V> recordGenerator) {
        this(numOfProducer, numOfConsumer,
                DEFAULT_PRODUCER_PROPERTY_FILE_NAME, DEFAULT_CONSUMER_PROPERTY_FILE_NAME,
                recordGenerator, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT_UNIT);
    }

    public GenericRunner(int numOfProducer, int numOfConsumer,
                         String producerPropertyFileName, String consumerPropertyFileName,
                         ProducerRecordGenerator<K, V> recordGenerator,
                         int timeout, TimeUnit timeUnit) {
        this.numOfProducer = numOfProducer;
        this.numOfConsumer = numOfConsumer;
        this.producerPropertyFileName = producerPropertyFileName;
        this.consumerPropertyFileName = consumerPropertyFileName;
        this.recordGenerator = recordGenerator;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    @Override
    public void run() {
        try {
            ExecutorService producerExecutor = Executors.newFixedThreadPool(numOfProducer);
            for (int i = 0; i < numOfProducer; i++) {
                Properties properties = PropertyUtils.loadProperties(producerPropertyFileName);
                properties.put(ProducerConfig.CLIENT_ID_CONFIG, String.valueOf(i));
                producerExecutor.submit(new GenericProducer<>(properties, recordGenerator));
            }


            GenericConsumerManager consumerManager = new GenericConsumerManager(consumerPropertyFileName, numOfConsumer);
            consumerManager.startAll();

            producerExecutor.awaitTermination(timeout, timeUnit);
            consumerManager.stopAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

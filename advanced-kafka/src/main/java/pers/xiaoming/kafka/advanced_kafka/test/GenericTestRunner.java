package pers.xiaoming.kafka.advanced_kafka.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import pers.xiaoming.kafka.advanced_kafka.PropertyUtils;
import pers.xiaoming.kafka.advanced_kafka.consumer.GenericConsumerManager;
import pers.xiaoming.kafka.advanced_kafka.producer.GenericProducer;
import pers.xiaoming.kafka.advanced_kafka.producer.ProducerRecordGenerator;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GenericTestRunner<K, V> extends Thread {
    private final int numOfProducer;
    private final int numOfConsumer;
    private final String producerPropertyFileName;
    private final String consumerPropertyFileName;
    private final ProducerRecordGenerator<K, V> recordGenerator;

    private final int timeout;
    private final TimeUnit timeUnit;

    public GenericTestRunner(int numOfProducer, int numOfConsumer,
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
            log.error(e.getMessage());
        }
    }
}

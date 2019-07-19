package pers.xiaoming.kafka.basic_kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final boolean isAsync;

    public Producer(String propertyFileName, boolean isAsync) throws IOException {
        this.isAsync = isAsync;

        Properties properties = PropertyUtils.loadProperties(propertyFileName);
        this.topic = properties.getProperty("topic");
        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync) {
                producer.send(new ProducerRecord<>(topic, messageNo, messageStr),
                        new DemoCallBack(startTime, messageNo, messageStr));
            } else {
                try {
                    producer.send(new ProducerRecord<>(topic, messageNo, messageStr)).get();
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    log.info("Message No: {},  {}, has been set in {} ms",
                            messageNo, messageStr, elapsedTime);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            messageNo++;
        }
    }

    private class DemoCallBack implements Callback {
        private final long startTime;
        private final int key;
        private final String message;

        DemoCallBack(long startTime, int key, String message) {
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

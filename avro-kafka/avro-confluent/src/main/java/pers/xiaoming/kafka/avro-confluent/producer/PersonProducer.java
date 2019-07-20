//package pers.xiaoming.kafka.avro.producer;
//
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.producer.Callback;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//
//import java.io.IOException;
//import java.time.Duration;
//import java.util.Properties;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@Slf4j
//public class PersonProducer extends Thread implements AutoCloseable {
//    private final KafkaProducer<Integer, Person> producer;
//    private final String topic;
//    private final AtomicInteger no;
//
//    public PersonProducer(Properties properties) throws IOException {
//        this.no = new AtomicInteger(0);
//
//        this.topic = properties.getProperty("topic");
//        this.producer = new KafkaProducer<>(properties);
//    }
//
//    @Override
//    public void run() {
//        while (true) {
//            int id = no.getAndIncrement();
//            Person person = new Person(id, "Person_" + id);
//            long startTime = System.currentTimeMillis();
//            producer.send(new ProducerRecord<>(topic, id, person),
//                    new MyCallBack(startTime, id, person));
//
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//        if (producer != null) {
//            producer.flush();
//            producer.close(Duration.ofMillis(5_000));
//        }
//    }
//
//    @RequiredArgsConstructor
//    private class MyCallBack implements Callback {
//        private final long startTime;
//        private final int key;
//        private final Person person;
//
//        @Override
//        public void onCompletion(RecordMetadata metadata, Exception exception) {
//            long elapsedTime = System.currentTimeMillis() - startTime;
//
//            if (metadata == null && exception != null) {
//                log.error("Failed to send message: Message No: {},  {} in {} ms. Exception {}", key, person.toString(), elapsedTime, exception);
//            }
//
//            if (metadata != null) {
//                log.info("Message No: {},  {}, has been set to partition {}, offset {} in {} ms",
//                        key, person.toString(), metadata.partition(), metadata.offset(), elapsedTime);
//            }
//        }
//    }
//}

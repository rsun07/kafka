package pers.xiaoming.kafka.advanced_kafka.producer;

import javafx.util.Pair;

public interface ProducerRecordGenerator<K, V> {
    Pair<K, V> nextRecord();
}

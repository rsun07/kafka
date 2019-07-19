package pers.xiaoming.kafka.advanced_kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PersonPartitioner implements Partitioner {
    private final Set<Integer> vipIdSet;

    public PersonPartitioner() {
        this.vipIdSet = new HashSet<>();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        final List<PartitionInfo> partitionInfoList = cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfoList.size();

        final int id = Integer.valueOf(String.valueOf(key));

        if (vipIdSet.contains(id)) {
            return 0;
        } else {
            return (id % partitionCount) + 1;
        }
    }

    @Override
    public void close() {

    }

    /**
     *
     * @param configs is the kafka config
     */
    @Override
    public void configure(Map<String, ?> configs) {
        final String vips = (String) configs.get("partitioner.vips");
        Arrays.stream(vips.split(",")).forEach(i -> vipIdSet.add(Integer.valueOf(i)));
    }
}

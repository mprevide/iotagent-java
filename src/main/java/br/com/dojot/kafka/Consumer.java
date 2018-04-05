package br.com.dojot.kafka;

import br.com.dojot.config.Config;

import java.util.*;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;


public class Consumer implements Runnable {
    Logger mLogger = Logger.getLogger(Consumer.class);

    private static final String DATA_BROKER_DEFAULT = "http://" + Config.getInstance().getDataBrokerAddress();

    private KafkaConsumer<String, String> mConsumer;
    private Map<String, Function<String, Integer>> mCallbacks;
    private List<String> mTopics;

    Consumer(String tenant, String subject, Boolean global, String brokerManager) {
        String broker = brokerManager != null ? brokerManager : DATA_BROKER_DEFAULT;

        this.mCallbacks = new HashMap<>();
        this.mTopics = new ArrayList<>();

        String topic = TopicProvider.getInstance().getTopic(subject, tenant, broker, global);

        if (topic.length() > 0) {
            this.mTopics.add(topic);
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getInstance().getKafkaAddress());
        props.put("group.id", Config.getInstance().getKafkaDefaultGroupId());
        props.put("session.timeout.ms", Config.getInstance().getKafkaDefaultSessionTimeout());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.mConsumer = new KafkaConsumer<>(props);
    }

    public void addCallback(String event, Function<String, Integer> callback) {
        mCallbacks.put(event, callback);
    }

    private void handleMessage(String message) {
        if (mCallbacks.containsKey("message")) {
            mCallbacks.get("message").apply(message);
        }
    }

    @Override
    public void run() {
        try {
            mLogger.info("Consumer thread is started and running...");

            this.mConsumer.subscribe(this.mTopics, new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    mLogger.info("topic-partitions are revoked from this consumer: " + Arrays.toString(partitions.toArray()));
                }
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    mLogger.info("topic-partitions are assigned to this consumer: " + Arrays.toString(partitions.toArray()));

                    if (mCallbacks.containsKey("connect")) {
                        mCallbacks.get("connect").apply("");
                    } else {
                        mLogger.warn("No defined callback for partitions assignment: event name connect");
                    }
                }
            });

            while (true) {
                ConsumerRecords<String, String> records = mConsumer.poll(10000);
                for (ConsumerRecord<String, String> record : records) {
                    handleMessage(record.value());
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            mConsumer.close();
        }
    }

    public void shutdown() {
        mConsumer.wakeup();
    }
}

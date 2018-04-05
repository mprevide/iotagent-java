package br.com.dojot.kafka;

import br.com.dojot.config.Config;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Properties;

public class Producer {
    Logger mLogger = Logger.getLogger(Producer.class);
    private static final String DATA_BROKER_DEFAULT = "http://" + Config.getInstance().getDataBrokerAddress();

    private String mBrokerManager;
    private KafkaProducer<String, String> mProducer;

    public Producer(String brokerManager) {
        this.mBrokerManager = brokerManager != null ? brokerManager : DATA_BROKER_DEFAULT;

        // create instance for properties to access producer configs
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getInstance().getKafkaAddress());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        mProducer = new KafkaProducer<>(props);
    }

    public void sendEvent(String tenant, String subject, JSONObject eventData) {
        String topic = TopicProvider.getInstance().getTopic(subject, tenant, this.mBrokerManager, false);
        if (topic.length() > 0) {
            JSONArray eventDataArray = new JSONArray().put(eventData.toString());
            mLogger.debug("Topic [" + topic + "] - Sending message:" + eventData.toString());
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, eventData.toString());
            this.mProducer.send(producerRecord, new ProducerSendCallback());
        }
    }

    private class ProducerSendCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                mLogger.error("Error producing to topic " + recordMetadata.topic());
                e.printStackTrace();
            } else {
                mLogger.debug("Producer send OK to topic " + recordMetadata.topic());
            }
        }
    }
}

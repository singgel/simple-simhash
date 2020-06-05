package com.singgel.simhash;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author singgel
 * @description
 * @created_at: 2020-06-02 14:06
 **/
public class KafkaHelper {



    public static Properties getConsumerProps(String group, String brokers) {
        Properties props = new Properties();

        props.put("bootstrap.servers", brokers);
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1000000");

        return props;
    }

    private static Properties getProducerProps(String brokers) {
        Properties props = new Properties();

        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 100);
        props.put("buffer.memory", 33554432);

        return props;
    }


    public static String timestmp2date(Long timestamp) {
        String str = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp);
        str = sdf.format(date);
        return str;
    }

    public static <T, V> KafkaConsumer<? extends Deserializer<T>, ? extends Deserializer<V>> getConsumer(String group, String brokers, Deserializer<T> key, Deserializer<V> value) {
        KafkaConsumer consumer = new KafkaConsumer<>(getConsumerProps(group, brokers), key, value);
        return consumer;
    }

    public static <T, V> KafkaProducer<? extends Serializer<T>, ? extends Serializer<V>> getProducer(String brokers, Serializer<T> key, Serializer<V> value) {
        KafkaProducer producer = new KafkaProducer<>(KafkaHelper.getProducerProps(brokers), key, value);
        return producer;
    }

}

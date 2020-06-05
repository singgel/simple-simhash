package com.singgel.simhash;

import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Arrays;

/**
 * @author singgel
 * @description
 * @created_at: 2020-06-01 14:05
 **/
@Slf4j
public class StreamRunner {

    private static final String BROKERS = ConfigFactory.load().getConfig("kafka").getString("broker");
    private static final String INPUT_TOPIC = ConfigFactory.load().getConfig("kafka").getString("input_topic");

    private static KafkaConsumer consumer = KafkaHelper.getConsumer("status_simhash-1", BROKERS, Constants.STR_DE, Constants.STR_DE);
    private static KafkaProducer producer = KafkaHelper.getProducer(BROKERS, Constants.STR_SER, Constants.STR_SER);


    public static void main(String[] args) {
        Processor processor = new Processor(producer);
        try {
            consumer.subscribe(Arrays.asList(INPUT_TOPIC));
            log.info("begin consume:...");
            while (true) {
                ConsumerRecords<String, String> statuses = consumer.poll(2000);
                for (ConsumerRecord<String, String> status : statuses) {
                    processor.process(status);
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            log.error("consumer exception", e);
        } finally {
            consumer.close();
        }

    }
}

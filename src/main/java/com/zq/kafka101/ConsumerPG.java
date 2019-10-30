package com.zq.kafka101;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerPG {

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String TOPIC = "test";
  private static final String GROUP_ID = String.valueOf(System.currentTimeMillis());
  private static final int POLLING_TIME = 100;
  private static final String OFFSET_RESET = "latest";

  private static Properties buildKafkaProperties() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
    return props;
  }

  public static void main(String args[]) {
    Properties props = buildKafkaProperties();
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
    List<String> topics = Arrays.asList(TOPIC);
    kafkaConsumer.subscribe(topics);
    while (true) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(POLLING_TIME));
      if (!records.isEmpty()) {
        ConsumerRecord<String, String> kafkaConsumerRecord = records.iterator().next();
        System.out.println(kafkaConsumerRecord.value());
      }
    }
  }
}

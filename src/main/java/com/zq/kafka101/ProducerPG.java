package com.zq.kafka101;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerPG {

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String TOPIC = "test";

  private static Properties buildKafkaProperties() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
    return props;
  }

  public static void main(String args[]) throws ExecutionException, InterruptedException {
    Properties props = buildKafkaProperties();
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
    while (true) {
      Scanner scanner = new Scanner(System.in);
      String input = scanner.nextLine();
      if ("EXIT".equalsIgnoreCase(input)) {
        break;
      }
      ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, input);
      Future<RecordMetadata> future = kafkaProducer.send(record);
      RecordMetadata recordMetadata = future.get();
      System.out.println("Message published on " +
          recordMetadata.topic() + " on partition number " + recordMetadata.partition() + " at "
          + recordMetadata.offset() + " offset.");
    }
  }
}

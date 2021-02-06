package com.knoldus;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }

    public static void consumer() {
        String FMessage = null;
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.knoldus.UserDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("User");
        kafkaConsumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords<String, User> messages = kafkaConsumer.poll(Duration.ofSeconds(1000));
                ObjectMapper mapper = new ObjectMapper();
                for (ConsumerRecord<String, User> message : messages) {
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", message.topic(), message.partition(), message.value().toString()));
                    System.out.println("Message received " + message.value().toString());
                    System.out.println(mapper.writeValueAsString(message.value()));
                    FMessage = new String(mapper.writeValueAsString(message.value())) + "\n";
                    writeDataToFile(FMessage);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    private static void writeDataToFile(String FMessage) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("Objects.txt", true));
        writer.append(FMessage);
        writer.close();
    }
}

class ConsumerListener implements Runnable {
    @Override
    public void run() {
        Consumer.consumer();
    }
}
package com.im.kl1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaClient {

    /**
     *
     */
    private static final String HELLO_WORLD = "Hello World!!";

    public Consumer<String, String> consumer(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092,localhost:39092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        return consumer;
    }

    public void publish(String topic, String msg) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:29092,localhost:39092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
        producer.send(record);
        producer.close();
    }

    public void consumerPool(String topicName, Consumer<String, String> consumer) {
        // print the topic name
        Runnable a = new Runnable() {

            public void run() {
                System.out.println("pooling to topic " + topicName);
                int i = 0;

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : records) {
                        // print the offset,key and value for the consumer records.
                        System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(),
                                record.value());
                    }
                    consumer.commitAsync(); 
                }
            }
        };

        Thread t = new Thread(a);
        t.start();

    }

    public void createTopic(String topicName, int partitions, int replicas) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092");
        AdminClient adminClient = AdminClient.create(properties);
        adminClient.close(Duration.ofSeconds(30));

        CreateTopicsResult newTopic = adminClient
                .createTopics(Collections.singletonList(new NewTopic(topicName, partitions, (short) replicas)));

    }

    public static void main(String[] args) {
        KafkaClient p = new KafkaClient();

        String topicName = "testTopic";
        p.createTopic(topicName, 1, 2);

        p.consumerPool(topicName, p.consumer(topicName));

        p.publish(topicName, HELLO_WORLD);
        System.out.println("Published message 1");

        p.publish(topicName, HELLO_WORLD);
        System.out.println("Published message 2");

    }
}
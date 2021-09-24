package com.im.kl1;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer1 {

    private Producer<String, String> producer;

    public void init() {
        producer = new KafkaProducer<>(getKafkaProps());
    }

    public void publish(String topic, String msg)
    {
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic, msg);
        producer.send(record);
    }

    public Properties getKafkaProps() {
        Properties props = new Properties();

        // Assign localhost id
        props.put("bootstrap.servers", "localhost:29092,localhost:39092");

        // Set acknowledgements for producer requests.
        props.put("acks", "all");

        // If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        // Specify buffer size in config
        props.put("batch.size", 16384);

        // Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        // The buffer.memory controls the total amount of memory available to the
        // producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serializa-tion.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serializa-tion.StringSerializer");

        return props;
    }
}
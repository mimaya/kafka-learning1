package com.im.kl1;

import org.junit.Test;

public class KafkaClientTest {
    //@Test
    public void testPublish() {

        KafkaClient p1 = new KafkaClient();

        String topic = "testTopic";
        p1.createTopic(topic, 1, 2);
        p1.publish("testTopic", "Hello World !!");
    }
}

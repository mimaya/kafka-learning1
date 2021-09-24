package com.im.kl1;

import org.junit.Test;

public class Producer1Test {
    @Test
    public void testPublish() {

        Producer1 p1 = new Producer1();
        p1.init();

        String topic = "testTopic";
        p1.createTopic(topic, 1, 2);
        p1.publish("testTopic", "Hello World !!");
    }
}

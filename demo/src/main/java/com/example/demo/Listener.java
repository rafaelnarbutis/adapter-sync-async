package com.example.demo;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class Listener {
    @KafkaListener(topics = "command", groupId = "my-group-id")
    public void listen(String message,  @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        System.out.println("Consumed key: " + key);
        System.out.println("Consumed message: " + message);

        Controller.REQUEST_MANAGER.get(key).complete("SUCCESS");
    }
}

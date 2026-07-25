package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@RestController
public class Controller {
    public static final ConcurrentHashMap<String, CompletableFuture<String>> REQUEST_MANAGER = new ConcurrentHashMap<>();
    @Autowired
    Producer producer;
    @PostMapping("/v1/simulate")
    public ResponseEntity<String> post(@RequestHeader("X-Correlation-ID") String correlationId, @RequestBody String request) {
        final CompletableFuture<String> pendingFuture = new CompletableFuture<>();

        REQUEST_MANAGER.put(correlationId, pendingFuture);
        producer.sendMessage(correlationId, request);

        return ResponseEntity.ok(pendingFuture.join());
    }
}

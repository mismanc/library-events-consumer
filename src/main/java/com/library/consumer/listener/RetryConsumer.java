package com.library.consumer.listener;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.library.consumer.service.LibraryEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class RetryConsumer {

    private final LibraryEventService libraryEventService;

    @KafkaListener(topics = {"${topics.retry}"}, groupId = "retry-listener-group", autoStartup = "${retryListener.startup:true}")
    public void onMessage(ConsumerRecord<Integer, String> records) {
        log.info("Consumer record in RETRY: {} ", records);
        records.headers().forEach(header -> log.info("Key: {} , Value: {}", header.key(), new String(header.value())));
        try {
            libraryEventService.processLibraryEvent(records);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}

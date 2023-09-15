package com.library.consumer.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.library.consumer.service.LibraryEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerListener {

    private final LibraryEventService libraryEventService;

    @KafkaListener(topics = {"library-events"}, id = "foo")
    public void onMessage(ConsumerRecord<Integer, String> records) {
        log.info("Consumer record: {} ", records);
        try {
            libraryEventService.processLibraryEvent(records);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}

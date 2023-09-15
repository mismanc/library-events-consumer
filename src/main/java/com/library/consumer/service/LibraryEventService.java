package com.library.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.library.consumer.domain.LibraryEvent;
import com.library.consumer.repository.LibraryEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventService {

    private final ObjectMapper objectMapper;
    private final LibraryEventRepository libraryEventRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> record) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(record.value(), LibraryEvent.class);
        log.info("libraryEvent : {} ", libraryEvent);
        switch (libraryEvent.getLibraryEventType()) {
            case NEW -> save(libraryEvent);
            case UPDATE -> {
                validateLibraryEvent(libraryEvent);
                save(libraryEvent);
            }
            default -> log.info("Invalid event type");
        }
    }

    private void validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library Event Id is missing");
        }

        Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
        if (libraryEventOptional.isEmpty()) {
            throw new IllegalArgumentException("Not a valid library event id");
        }
        log.info("Validation is successful for the library event : {} ", libraryEvent);
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        LibraryEvent saved = libraryEventRepository.save(libraryEvent);
        log.info("libraryEvent saved : {} ", saved);
    }

}

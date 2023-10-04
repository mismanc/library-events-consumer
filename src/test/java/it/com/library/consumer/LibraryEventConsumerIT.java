package com.library.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.library.consumer.domain.Book;
import com.library.consumer.domain.LibraryEvent;
import com.library.consumer.domain.LibraryEventType;
import com.library.consumer.listener.KafkaConsumerListener;
import com.library.consumer.model.BookDTO;
import com.library.consumer.model.LibraryEventDTO;
import com.library.consumer.repository.LibraryEventRepository;
import com.library.consumer.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
public class LibraryEventConsumerIT {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @SpyBean
    private LibraryEventService serviceSpy;

    @SpyBean
    private KafkaConsumerListener consumerListenerSpy;

    @BeforeEach
    public void setUp() {
        for (MessageListenerContainer listenerContainer : endpointRegistry.getAllListenerContainers()) {
            ContainerTestUtils.waitForAssignment(listenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    public void tearDown() {
        libraryEventRepository.deleteAll();
    }

    @Test
    public void publishNewLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        LibraryEventDTO dto = LibraryEventDTO.builder().libraryEventType(LibraryEventType.NEW)
                .book(BookDTO.builder().id(125).author("Alija Izetbegovic").name("Islam Between East And West").build())
                .build();

        kafkaTemplate.sendDefault(objectMapper.writeValueAsString(dto)).get();

        // when
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        // then
        verify(consumerListenerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        List<LibraryEvent> libraryEventList = libraryEventRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(125, libraryEvent.getBook().getId());
        });
    }

    @Test
    public void publishUpdateEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventType(LibraryEventType.NEW)
                .book(Book.builder().id(125).author("Islam Between East And West").name("Alija Izetbegovic").build())
                .build();
        LibraryEvent savedLibraryEvent = libraryEventRepository.save(libraryEvent);

        Book updatedBook = Book.builder().id(125).author("Alija Izetbegovic").name("Islam Between East And West").build();
        savedLibraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        savedLibraryEvent.setBook(updatedBook);

        kafkaTemplate.sendDefault(savedLibraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(savedLibraryEvent)).get();

        // when
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        // then
        verify(consumerListenerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(savedLibraryEvent.getLibraryEventId());
        assertTrue(libraryEventOptional.isPresent());
        assertEquals(updatedBook.getName(), libraryEventOptional.get().getBook().getName());
        assertEquals(updatedBook.getAuthor(), libraryEventOptional.get().getBook().getAuthor());
    }

    @Test
    public void publishUpdateNullIdEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventType(LibraryEventType.UPDATE)
                .book(Book.builder().id(125).author("Islam Between East And West").name("Alija Izetbegovic").build())
                .build();

        kafkaTemplate.sendDefault(objectMapper.writeValueAsString(libraryEvent)).get();

        // when
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        // then
        verify(consumerListenerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

    }

    @Test
    public void publishUpdate999IdEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(999).libraryEventType(LibraryEventType.UPDATE)
                .book(Book.builder().id(125).author("Islam Between East And West").name("Alija Izetbegovic").build())
                .build();

        kafkaTemplate.sendDefault(objectMapper.writeValueAsString(libraryEvent)).get();

        // when
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        // then
        verify(consumerListenerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

    }

}

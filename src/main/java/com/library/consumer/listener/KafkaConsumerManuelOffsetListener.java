package com.library.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Objects;

// @Component
@Slf4j
public class KafkaConsumerManuelOffsetListener implements AcknowledgingMessageListener<Integer, String> {

    @KafkaListener(topics = {"library-events"} , id = "lib.group")
    @Override
    public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
        log.info("Consumer record: {} ", data);
        Objects.requireNonNull(acknowledgment).acknowledge();
    }

}

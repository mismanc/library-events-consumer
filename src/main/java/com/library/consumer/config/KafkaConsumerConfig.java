package com.library.consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    public KafkaConsumerConfig(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public DeadLetterPublishingRecoverer publishingRecover() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (r, e) -> {
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            }

            return new TopicPartition(deadLetterTopic, r.partition());
        });

        return recoverer;
    }


    public DefaultErrorHandler errorHandler() {

        List<Class<IllegalArgumentException>> exceptionsToIgnore = List.of(IllegalArgumentException.class);
        List<Class<RecoverableDataAccessException>> exceptionsToRetry = List.of(RecoverableDataAccessException.class);
        FixedBackOff fb = new FixedBackOff(1000L, 2);
        ExponentialBackOffWithMaxRetries exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1_000L);
        exponentialBackOff.setMultiplier(2.0);
        exponentialBackOff.setMaxInterval(2_000L);

        DefaultErrorHandler deh = new DefaultErrorHandler(
                publishingRecover(),
                // fb
                exponentialBackOff);
        // exceptionsToIgnore.forEach(deh::addNotRetryableExceptions);
        exceptionsToRetry.forEach(deh::addRetryableExceptions);
        deh.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt);
        }));

        return deh;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
                                                                                ConsumerFactory<Object, Object> kafkaConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }


}

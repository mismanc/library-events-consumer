package com.library.consumer.config;

import com.library.consumer.domain.FailureType;
import com.library.consumer.service.FailureRecordService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private FailureRecordService failureRecordService;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    public DeadLetterPublishingRecoverer publishingRecover() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (r, e) -> {
            log.error("Exception in publishingRecover : {} ", e.getMessage());
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
                consumerRecordRecoverer,
                // publishingRecover(),
                // fb
                exponentialBackOff);
        exceptionsToIgnore.forEach(deh::addNotRetryableExceptions);
        // exceptionsToRetry.forEach(deh::addRetryableExceptions);
        deh.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt);
        }));

        return deh;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
        log.error("Exception in consumerRecordRecover : {} ", e.getMessage());
        ConsumerRecord<Integer, String> record = (ConsumerRecord<Integer, String>) consumerRecord;
        if (e.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside Recovery");
            failureRecordService.saveFailedRecord(record, e, FailureType.RETRY);
        }else {
            log.info("Inside Non - Recovery");
            failureRecordService.saveFailedRecord(record, e, FailureType.DEAD);
        }
    };

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

package com.library.consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConsumerConfig {


    public DefaultErrorHandler errorHandler(){

        List<Class<IllegalArgumentException>> exceptionsToIgnore = List.of(IllegalArgumentException.class);
        FixedBackOff fb = new FixedBackOff(1000L, 2);
        DefaultErrorHandler deh = new DefaultErrorHandler(fb);
        exceptionsToIgnore.forEach(deh::addNotRetryableExceptions);
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

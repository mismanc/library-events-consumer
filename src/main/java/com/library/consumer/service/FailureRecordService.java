package com.library.consumer.service;

import com.library.consumer.domain.FailureRecord;
import com.library.consumer.domain.FailureType;
import com.library.consumer.repository.FailureRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class FailureRecordService {

    private final FailureRecordRepository failureRecordRepository;


    public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, FailureType failureType) {
        FailureRecord failureRecord = FailureRecord.builder().topic(consumerRecord.topic())
                .theKey(consumerRecord.key())
                .errorRecord(consumerRecord.value())
                .offsetValue(consumerRecord.offset())
                .exception(e.getCause().getMessage())
                .status(failureType.name())
                .partition(consumerRecord.partition())
                .build();
        failureRecordRepository.save(failureRecord);
    }
}

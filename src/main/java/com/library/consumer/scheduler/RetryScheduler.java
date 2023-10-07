package com.library.consumer.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.library.consumer.domain.FailureRecord;
import com.library.consumer.domain.FailureType;
import com.library.consumer.repository.FailureRecordRepository;
import com.library.consumer.service.LibraryEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class RetryScheduler {

    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventService libraryEventService;

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {
        log.info("Retrying Failed Records Started");
        List<FailureRecord> failureRecordList = failureRecordRepository.findAllByStatus(FailureType.RETRY.name());
        failureRecordList.forEach(failureRecord -> {
            ConsumerRecord<Integer,String> record = buildConsumerRecord(failureRecord);
            try {
                libraryEventService.processLibraryEvent(record);
                failureRecord.setStatus(FailureType.SUCCESS.name());
                failureRecordRepository.save(failureRecord);
            } catch (JsonProcessingException e) {
                log.error("Exception in retryFailedRecords : {}", e.getMessage(), e);
            }
        });
        log.info("Retrying Failed Records Completed");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffsetValue(),
                failureRecord.getTheKey(),
                failureRecord.getErrorRecord());

    }
}

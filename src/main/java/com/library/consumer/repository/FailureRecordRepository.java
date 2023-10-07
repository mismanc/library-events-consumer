package com.library.consumer.repository;

import com.library.consumer.domain.FailureRecord;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {

    List<FailureRecord> findAllByStatus(String name);

}

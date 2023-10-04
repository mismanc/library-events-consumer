package com.library.consumer.model;

import com.library.consumer.domain.LibraryEventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record LibraryEventDTO(
        Integer libraryEventId,
        LibraryEventType libraryEventType,
        @NotNull
        @Valid
        BookDTO book) {


}

package com.library.consumer.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record BookDTO(
        @NotNull
        Integer id,
        @NotBlank
        String name,
        @NotBlank
        String author) {
}

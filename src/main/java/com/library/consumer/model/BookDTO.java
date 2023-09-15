package com.library.consumer.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record BookDTO(
        @NotNull
        Integer id,
        @NotBlank
        String name,
        @NotBlank
        String author) {
}

package com.ronaldsuwandi.model;

import java.time.Instant;

public record UserStepsDaily(
        String userId,
        int steps,
        Instant timestamp,
        String label
) {
}

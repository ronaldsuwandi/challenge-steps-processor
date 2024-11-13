package com.ronaldsuwandi.model;

import java.time.Instant;

public record UserActivityNormalized(
        String userId,
        String firstName,
        String lastName,
        int steps,
        Double latitude,
        Double longitude,
        String pincode,
        Instant timestamp
) {

}

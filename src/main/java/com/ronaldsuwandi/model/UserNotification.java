package com.ronaldsuwandi.model;

import java.time.Instant;

public record UserNotification(
        String userId,
        String notificationId,
        String payload,
        Instant timestamp) {

}

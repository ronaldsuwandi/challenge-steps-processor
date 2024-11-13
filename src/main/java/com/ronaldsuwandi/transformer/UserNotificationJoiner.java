package com.ronaldsuwandi.transformer;

import com.ronaldsuwandi.model.UserActivityNormalized;
import com.ronaldsuwandi.model.UserNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public class UserNotificationJoiner {
    private static Logger logger = LoggerFactory.getLogger(UserNotificationJoiner.class);
    public static final String STORE_NAME = "user-notification-id-store";

    public static UserNotification joinWithWeeklyAggregatedLabel(UserActivityNormalized userActivity, String userWeeklyAggregatedLabel) {
        logger.trace("weeklyLabel = {}", userWeeklyAggregatedLabel);

        if ("medium_or_high".equals(userWeeklyAggregatedLabel)) {
            return new UserNotification(
                    userActivity.userId(),
                    "UNIQUE-NOTIFICATION-ID", // TODO
                    "Time for a juice nearby!" + userActivity.userId(),
                    Instant.now()
            );
        }
        return null;
    }

    public static UserNotification debounceNotification(UserNotification userNotification, UserNotification existingUserNotification) {
        if (existingUserNotification != null) {
            Duration diff = Duration.between(userNotification.timestamp(), existingUserNotification.timestamp());
            if (Math.abs(diff.getSeconds()) > 10) { // TODO - hardcoded for now
                return userNotification;
            }
            logger.debug("Debounced. Too soon, wait again before sending notification to {}", userNotification.userId());
            return null;
        }
        return userNotification;
    }
}

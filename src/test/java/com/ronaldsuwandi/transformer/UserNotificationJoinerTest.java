package com.ronaldsuwandi.transformer;

import com.ronaldsuwandi.model.UserActivityNormalized;
import com.ronaldsuwandi.model.UserNotification;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

public class UserNotificationJoinerTest {

    @Test
    public void testJoinWithWeeklyAggregatedLabel_MediumOrHigh() {
        UserActivityNormalized userActivity = new UserActivityNormalized("user1", "first", "last", 3000, null, null, null, Instant.now());
        String userWeeklyAggregatedLabel = "medium_or_high";

        UserNotification notification = UserNotificationJoiner.joinWithWeeklyAggregatedLabel(userActivity, userWeeklyAggregatedLabel);

        assertNotNull(notification);
        assertEquals("user1", notification.userId());
    }

    @Test
    public void testJoinWithWeeklyAggregatedLabel_Low() {
        UserActivityNormalized userActivity = new UserActivityNormalized("user1", "first", "last", 3000, null, null, null, Instant.now());
        String userWeeklyAggregatedLabel = "low";

        UserNotification notification = UserNotificationJoiner.joinWithWeeklyAggregatedLabel(userActivity, userWeeklyAggregatedLabel);

        assertNull(notification, "Notification should be null for low label");
    }

    @Test
    public void testDebounceNotification_NoExistingNotification() {
        UserNotification newNotification = new UserNotification(
                "user1", "NOTIF-ID", "Time for a juice nearby!", Instant.now());

        UserNotification result = UserNotificationJoiner.debounceNotification(newNotification, null);

        assertNotNull(result);
        assertEquals(newNotification, result);
    }

    @Test
    public void testDebounceNotification_ExistingNotificationTooRecent() {
        Instant now = Instant.now();
        UserNotification newNotification = new UserNotification("user1", "NOTIF-ID", "Time for a juice nearby!", now);
        UserNotification existingNotification = new UserNotification("user1", "NOTIF-ID", "Recent juice offer", now.minusSeconds(3));

        UserNotification result = UserNotificationJoiner.debounceNotification(newNotification, existingNotification);

        assertNull(result, "Notification should be debounced if existing notification is too recent");
    }

    @Test
    public void testDebounceNotification_ExistingNotificationOldEnough() {
        Instant now = Instant.now();
        UserNotification newNotification = new UserNotification("user1", "NOTIF-ID", "Time for a juice nearby!", now);
        UserNotification existingNotification = new UserNotification("user1", "NOTIF-ID", "Older juice offer", now.minusSeconds(3600)); // 1 hour ago

        UserNotification result = UserNotificationJoiner.debounceNotification(newNotification, existingNotification);

        assertNotNull(result, "Notification should not be debounced if existing notification is old enough");
        assertEquals(newNotification, result);
    }
}

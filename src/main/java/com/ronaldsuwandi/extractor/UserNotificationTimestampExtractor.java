package com.ronaldsuwandi.extractor;

import com.ronaldsuwandi.model.UserNotification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class UserNotificationTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        // Return the timestamp from the message itself
        UserNotification userNotification = (UserNotification) record.value();
        if (userNotification == null) {
            return previousTimestamp;
        }
        return userNotification.timestamp().toEpochMilli();
    }
}

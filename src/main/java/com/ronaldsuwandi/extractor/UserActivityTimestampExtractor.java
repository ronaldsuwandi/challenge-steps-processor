package com.ronaldsuwandi.extractor;

import com.ronaldsuwandi.model.UserActivityNormalized;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;


public class UserActivityTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        // Return the timestamp from the message itself
        UserActivityNormalized userActivityNormalized = (UserActivityNormalized) record.value();
        if (userActivityNormalized == null) {
            return previousTimestamp;
        }
        return userActivityNormalized.timestamp().toEpochMilli();
    }
}

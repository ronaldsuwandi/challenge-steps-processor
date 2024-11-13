package com.ronaldsuwandi;

import com.ronaldsuwandi.extractor.UserActivityTimestampExtractor;
import com.ronaldsuwandi.extractor.UserNotificationTimestampExtractor;
import com.ronaldsuwandi.model.*;
import com.ronaldsuwandi.processor.ConsecutiveStepsProcessor;
import com.ronaldsuwandi.serde.JSONSerdeUtil;
import com.ronaldsuwandi.serde.NavigableMapSerde;
import com.ronaldsuwandi.transformer.UserNotificationJoiner;
import com.ronaldsuwandi.transformer.UserStepsAggregator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class TopologyBuilder {
    private static Logger logger = LoggerFactory.getLogger(TopologyBuilder.class);

    public static final String TOPIC_USER_ACTIVITY_IOS = "user-activity-ios";
    public static final String TOPIC_USER_ACTIVITY_ANDROID = "user-activity-android";
    public static final String TOPIC_USER_ACTIVITY_NORMALIZED = "user-activity-normalized";
    public static final String TOPIC_USER_STEPS_7_DAYS_AGGREGATED = "user-steps-7-days-aggregated";
    public static final String TOPIC_USER_NOTIFICATION_ID_TABLE = "user-notification-id-table";
    public static final String TOPIC_USER_NOTIFICATIONS = "user-notifications";


    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        setupNormalization(builder);
        setupStepsAggregations(builder);
        setupNotificationCreationStream(builder);
        setupNotificationStream(builder);

        return builder.build();
    }

    private void setupNormalization(StreamsBuilder builder) {

        KStream<String, UserActivityNormalized> inputStreamIos = builder.stream(TOPIC_USER_ACTIVITY_IOS,
                        Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityIos.class)))
                .mapValues(UserActivityNormalizer::normalize);
        KStream<String, UserActivityNormalized> inputStreamAndroid = builder.stream(TOPIC_USER_ACTIVITY_ANDROID,
                        Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityAndroid.class)))
                .mapValues(UserActivityNormalizer::normalize);

        inputStreamIos
                .merge(inputStreamAndroid)
                .to(TOPIC_USER_ACTIVITY_NORMALIZED, Produced.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityNormalized.class)));
    }

    private void setupStepsAggregations(StreamsBuilder builder) {
        TimeWindows dailyWindow = TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofHours(1))
                .advanceBy(Duration.ofDays(1));

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ConsecutiveStepsProcessor.STORE_NAME),
                Serdes.String(),
                new NavigableMapSerde());

        builder.addStateStore(storeBuilder);

        builder.stream(TOPIC_USER_ACTIVITY_NORMALIZED,
                        Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityNormalized.class))
                                .withTimestampExtractor(new UserActivityTimestampExtractor()))
                .groupByKey()
                .windowedBy(dailyWindow)
                .aggregate(
                        () -> new UserStepsDaily("", 0, null, ""),
                        new UserStepsAggregator(),
                        Materialized.<String, UserStepsDaily, WindowStore<Bytes, byte[]>>as(UserStepsAggregator.STORE_NAME)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JSONSerdeUtil.getSerde(UserStepsDaily.class)))
                .toStream()
                .peek(((key, value) -> {
                    logger.debug("Daily aggregated label for {} = {}", key, value);
                }))
                .process(ConsecutiveStepsProcessor::new, ConsecutiveStepsProcessor.STORE_NAME)
                .to(TOPIC_USER_STEPS_7_DAYS_AGGREGATED);
    }

    private void setupNotificationCreationStream(StreamsBuilder builder) {
        KTable<String, UserNotification> userNotificationIdTable = builder.table(TOPIC_USER_NOTIFICATION_ID_TABLE,
                Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserNotification.class))
                        .withTimestampExtractor(new UserNotificationTimestampExtractor()),
                Materialized.as(UserNotificationJoiner.STORE_NAME));

        KTable<String, String> userWeeklyStepsLabelTable = builder.table(TOPIC_USER_STEPS_7_DAYS_AGGREGATED,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, UserNotification> notificationStream = builder.stream(TOPIC_USER_ACTIVITY_NORMALIZED,
                        Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityNormalized.class))
                                .withTimestampExtractor(new UserActivityTimestampExtractor()))
                .join(userWeeklyStepsLabelTable, UserNotificationJoiner::joinWithWeeklyAggregatedLabel)
                .leftJoin(userNotificationIdTable, UserNotificationJoiner::debounceNotification)
                .filter((key, value) -> value != null);

        // update table to keep track of last notification message generated
        notificationStream.to(TOPIC_USER_NOTIFICATION_ID_TABLE,
                Produced.with(Serdes.String(), JSONSerdeUtil.getSerde(UserNotification.class)));

        // push downstream
        notificationStream.to(TOPIC_USER_NOTIFICATIONS,
                Produced.with(Serdes.String(), JSONSerdeUtil.getSerde(UserNotification.class)));
    }

    private void setupNotificationStream(StreamsBuilder builder) {
        builder.stream(TOPIC_USER_NOTIFICATIONS, Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserNotification.class)))
                .peek((key, value) -> {
                    // pretend send notification
                    logger.info("SENDING NOTIFICATION FOR USER={}, NOTIFICATION={}", key, value);
                });
    }
}

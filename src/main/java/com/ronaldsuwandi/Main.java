package com.ronaldsuwandi;

import com.ronaldsuwandi.model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "steps-processor");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "steps-processor-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        String stateDir = System.getenv().getOrDefault("KAFKA_STREAMS_STATE_DIR", "./store");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); // disable caching


        StreamsBuilder builder = new StreamsBuilder();
        // Transformation: normalize data format
        KStream<String, UserActivityNormalized> inputStreamIos = builder.stream("user-activity-ios",
                        Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityIos.class)))
                .mapValues(UserActivityNormalizer::normalize);
        KStream<String, UserActivityNormalized> inputStreamAndroid = builder.stream("user-activity-android", Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityAndroid.class)))
                .mapValues(UserActivityNormalizer::normalize);
        inputStreamIos
                .merge(inputStreamAndroid)
                .to("user-activity-normalized", Produced.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityNormalized.class)));

        // Define a daily time window, with retention of 7 days
        TimeWindows dailyWindow = TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofHours(1))
                .advanceBy(Duration.ofDays(1));

        // group by user id
        KTable<String, UserStepsDaily> userStepsAggregateTable = builder.stream("user-activity-normalized",
                        Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityNormalized.class))
                                .withTimestampExtractor(new UserActivityTimestampExtractor())
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)) // TODO for debugging
                .groupByKey()
                .windowedBy(dailyWindow)
                .aggregate(
                        () -> new UserStepsDaily("", 0, null, ""), // initialize
                        (userId, userActivity, aggregate) -> {

                            int newSteps = aggregate.steps() + userActivity.steps();
                            String label = getStepLabel(newSteps);
                            Instant day = userActivity.timestamp().truncatedTo(ChronoUnit.DAYS);

                            return new UserStepsDaily(
                                    userActivity.userId(),
                                    newSteps,
                                    day,
                                    label
                            );
                        },
                        Materialized.<String, UserStepsDaily, WindowStore<Bytes, byte[]>>as("daily-user-steps-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JSONSerdeUtil.getSerde(UserStepsDaily.class)))
                .toStream()
                .selectKey((windowedKey, value) -> windowedKey.key(), Named.as("removeWindowing"))
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue,
                        Named.as("userStepsTableReduce"),
                        Materialized.as("user-steps-store"));

//        userStepsAggregateTable.toStream().peek((key, value) -> {
//            System.out.println("TABLE -> key: " + key+", value: " + value);
//        }, Named.as("kaypoh-checktable-content"));
//

        KTable<String, UserNotification> userNotificationIdTable = builder.table("user-notification-id-table",
                Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserNotification.class))
                        .withTimestampExtractor(new UserNotificationTimestampExtractor())
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST),// TODO for debugging
                Materialized.as("user-notification-id-store"));


        KStream<String, UserNotification> notificationStream = builder.stream("user-activity-normalized",
                        Consumed.with(Serdes.String(), JSONSerdeUtil.getSerde(UserActivityNormalized.class))
                                .withTimestampExtractor(new UserActivityTimestampExtractor())
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .join(userStepsAggregateTable, (userActivity, userStepsDaily) -> {
                    if (userStepsDaily != null &&
                            (userStepsDaily.label().equals("daily_medium_stepper") || userStepsDaily.label().equals("daily_high_stepper"))) {
                        return new UserNotification(
                                userActivity.userId(),
                                "notification-uuid", // TODO
                                "payload for " + userActivity.userId(),
                                Instant.now()
                        );
                    }
                    return null;
                }).peek((key, value) -> {
                    System.out.println("JOIN1 -> NOTIFICATION! > " + key + ", value: " + value);
                }, Named.as("kaypoh2-print-first-join"))
                .leftJoin(userNotificationIdTable, (userNotification, existingUserNotification) -> {
                    System.out.println("JOIN2 (INSIDE) -> existing: " + existingUserNotification);
                    if (existingUserNotification != null) {
                        Duration diff = Duration.between(userNotification.timestamp(), existingUserNotification.timestamp());
                        System.out.println("HOURS DIFF = " + diff);
                        // TODO hard code 5 seconds gap for each notification
                        if (Math.abs(diff.getSeconds()) > 5) {
                            System.out.println("old notification will be replaced");
                            return userNotification;
                        } else {
                            System.out.println("recently sent notification, skip");
                            return null;
                        }
                    }
                    return userNotification;
                }).peek((key, value) -> {
                    System.out.println("JOIN2 (AFTER) -> NOTIFICATION! > " + key + ", value: " + value);
                }, Named.as("kaypoh2-print-second-join"))
                .filter((key, value) -> value != null);

        notificationStream.to("user-notification-id-table",
                Produced.with(Serdes.String(), JSONSerdeUtil.getSerde(UserNotification.class)));
        notificationStream.to("user-notifications",
                Produced.with(Serdes.String(), JSONSerdeUtil.getSerde(UserNotification.class))); // target topic for second topology/app



        Topology topology = builder.build();

        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setStateListener((state, state1) -> {
            if (state1.isRunningOrRebalancing()) {
                System.out.println("RUNNING!");
            }
        });

//        streams.setStateListener((state, state1) -> {
//            if (state1.isRunningOrRebalancing()) {
//                System.out.println("RUNNING!");
//
//                System.out.println("READING FROM STORE");
//                ReadOnlyWindowStore<String, UserStepsDaily> windowStore = streams.store(StoreQueryParameters.fromNameAndType("user-steps- store", QueryableStoreTypes.windowStore()));
//
//                // Iterate through the current window contents
//                for (KeyValueIterator<Windowed<String>, UserStepsDaily> it = windowStore.all(); it.hasNext(); ) {
//                    KeyValue<Windowed<String>, UserStepsDaily> entry = it.next();
//                    Windowed<String> windowedKey = entry.key;
//                    UserStepsDaily value = entry.value;
//
//                    // Extract the actual key (without the window suffix)
//                    String key = windowedKey.key();
//
//                    // Access the window start and end timestamps
//                    long windowStart = windowedKey.window().start();
//                    long windowEnd = windowedKey.window().end();
//
//                    System.out.println("Key: " + key + ", Value: " + value + ", Window: " + windowStart + " - " + windowEnd);
//                }
//
//
//            }
//        });


        // start stream
        streams.start();


        // Add shutdown hook for clean shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String getStepLabel(int steps) {
        if (steps < 1000) {
            return "daily_low_stepper";
        } else if (steps >= 1000 && steps < 5000) {
            return "daily_medium_stepper";
        } else {
            return "daily_high_stepper";
        }
    }

}

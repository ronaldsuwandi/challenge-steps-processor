package com.ronaldsuwandi.transformer;

import com.ronaldsuwandi.model.UserActivityNormalized;
import com.ronaldsuwandi.model.UserStepsDaily;
import org.apache.kafka.streams.kstream.Aggregator;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class UserStepsAggregator implements Aggregator<String, UserActivityNormalized, UserStepsDaily> {
    public static final String STORE_NAME = "daily-user-steps-store";
    public static String getStepLabel(int steps) {
        if (steps < 1000) {
            return "daily_low_stepper";
        } else if (steps < 5000) {
            return "daily_medium_stepper";
        } else {
            return "daily_high_stepper";
        }
    }

    @Override
    public UserStepsDaily apply(String key, UserActivityNormalized value, UserStepsDaily aggregate) {
        int newSteps = aggregate.steps() + value.steps();
        String label = getStepLabel(newSteps);
        Instant day = value.timestamp().truncatedTo(ChronoUnit.DAYS);
        return new UserStepsDaily(value.userId(), newSteps, day, label);
    }
}

package com.ronaldsuwandi.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record UserActivityAndroid(
        @JsonProperty("Person")
        Person person,
        @JsonProperty("Activity")
        Activity activity,
        @JsonProperty("Location")
        Location location,
        @JsonProperty("ts")
        Instant timestamp
) {
    public record Person(
            @JsonProperty("FirstName")
            String firstName,
            @JsonProperty("LastName")
            String lastName,
            @JsonProperty("Address")
            String address,
            @JsonProperty("Id")
            String id,
            @JsonProperty("Gender")
            String gender,
            @JsonProperty("Height")
            double height,
            @JsonProperty("Weight")
            double weight
    ) {
    }

    public record Activity(
            @JsonProperty("Dance_minutes")
            int danceMinutes,
            @JsonProperty("Yoga_minutes")
            int yogaMinutes,
            @JsonProperty("Sleep_hours")
            int sleepHours,
            @JsonProperty("walking_steps")
            int walkingSteps
    ) {
    }

    public record Location(
            Double lat,
            Double lon,
            String pincode
    ) {
    }
}

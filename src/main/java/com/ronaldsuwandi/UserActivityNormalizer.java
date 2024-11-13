package com.ronaldsuwandi;

import com.ronaldsuwandi.model.UserActivityAndroid;
import com.ronaldsuwandi.model.UserActivityIos;
import com.ronaldsuwandi.model.UserActivityNormalized;

public class UserActivityNormalizer {
        public static UserActivityNormalized normalize(UserActivityIos iosActivity) {
            return new UserActivityNormalized(
                    iosActivity.id(),
                    iosActivity.firstName(),
                    iosActivity.lastName(),
                    iosActivity.activity().walkingSteps(),
                    iosActivity.latitude(),
                    iosActivity.longitude(),
                    iosActivity.pincode(),
                    iosActivity.timestamp()
            );
        }

        public static UserActivityNormalized normalize(UserActivityAndroid androidActivity) {
            Double latitude = androidActivity.location() != null ? androidActivity.location().lat() : null;
            Double longitude = androidActivity.location() != null ? androidActivity.location().lon() : null;
            String pincode = androidActivity.location() != null ? androidActivity.location().pincode() : null;

            return new UserActivityNormalized(
                    androidActivity.person().id(),
                    androidActivity.person().firstName(),
                    androidActivity.person().lastName(),
                    androidActivity.activity().walkingSteps(),
                    latitude,
                    longitude,
                    pincode,
                    androidActivity.timestamp()
            );
        }
}

package com.goon.wsgateway.redis;

public final class RideChannelNames {

    private static final String REDIS_PREFIX = "ride:";
    private static final String REDIS_SUFFIX = ":loc";
    private static final String WS_PREFIX = "/topic/ride/";

    private RideChannelNames() {
    }

    public static String redisChannel(String rideId) {
        return REDIS_PREFIX + rideId + REDIS_SUFFIX;
    }

    public static String websocketTopic(String rideId) {
        return WS_PREFIX + rideId;
    }

    public static String extractRideIdFromRedisChannel(String channel) {

        if (channel == null
                || !channel.startsWith(REDIS_PREFIX)
                || !channel.endsWith(REDIS_SUFFIX)) {
            return null;
        }

        int start = REDIS_PREFIX.length();
        int end = channel.length() - REDIS_SUFFIX.length();

        if (end <= start) {
            return null;
        }

        String rideId = channel.substring(start, end);

        if (rideId.isBlank() || rideId.contains(":")) {
            return null;
        }

        return rideId;
    }

    public static String extractRideIdFromWebSocketTopic(String destination) {

        if (destination == null
                || !destination.startsWith(WS_PREFIX)) {
            return null;
        }

        String rideId = destination.substring(WS_PREFIX.length());

        if (rideId.isBlank() || rideId.contains("/")) {
            return null;
        }

        return rideId;
    }
}
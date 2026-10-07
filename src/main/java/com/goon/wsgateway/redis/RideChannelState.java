package com.goon.wsgateway.redis;

public record RideChannelState(
        int subscribers,
        Status status
) {

    public enum Status {
        SUBSCRIBING,
        SUBSCRIBED,
        FAILED
    }
}
package com.goon.wsgateway.redis;

import com.goon.wsgateway.websocket.RideMessageForwarder;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class RideRedisSubscriber {

    private final StatefulRedisPubSubConnection<String, String> connection;
    private final RideMessageForwarder forwarder;

    @PostConstruct
    public void init() {

        connection.addListener(
                new RedisPubSubAdapter<String, String>() {

                    @Override
                    public void message(String channel, String message) {
                        handle(channel, message);
                    }
                }
        );
    }

    private void handle(String channel, String message) {

        String rideId =
                RideChannelNames.extractRideIdFromRedisChannel(channel);

        if (rideId == null) {
            log.warn("Ignoring invalid Redis channel {}", channel);
            return;
        }

        forwarder.forward(rideId, message);
    }
}
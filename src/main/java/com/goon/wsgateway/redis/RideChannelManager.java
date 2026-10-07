package com.goon.wsgateway.redis;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class RideChannelManager {

    private final StatefulRedisPubSubConnection<String, String> connection;

    private final ConcurrentHashMap<String, RideChannelState> rides =
            new ConcurrentHashMap<>();

    public void subscribe(String rideId) {

        rides.compute(rideId, (id, state) -> {

            if (state == null) {
                subscribeRedis(id);

                return new RideChannelState(
                        1,
                        RideChannelState.Status.SUBSCRIBING
                );
            }

            if (state.status() == RideChannelState.Status.FAILED) {
                subscribeRedis(id);

                return new RideChannelState(
                        state.subscribers() + 1,
                        RideChannelState.Status.SUBSCRIBING
                );
            }

            return new RideChannelState(
                    state.subscribers() + 1,
                    state.status()
            );
        });
    }

    public void unsubscribe(String rideId) {

        rides.computeIfPresent(rideId, (id, state) -> {

            int remaining = state.subscribers() - 1;

            if (remaining > 0) {
                return new RideChannelState(
                        remaining,
                        state.status()
                );
            }

            if (state.status() == RideChannelState.Status.SUBSCRIBED) {
                unsubscribeRedis(id);
            }

            return null;
        });
    }

    private void subscribeRedis(String rideId) {

        String channel =
                RideChannelNames.redisChannel(rideId);

        connection.async()
                .ssubscribe(channel)
                .whenComplete((result, ex) -> {

                    if (ex != null) {

                        log.error(
                                "Failed to subscribe to {}",
                                channel,
                                ex
                        );

                        rides.computeIfPresent(
                                rideId,
                                (id, state) ->
                                        new RideChannelState(
                                                state.subscribers(),
                                                RideChannelState.Status.FAILED
                                        )
                        );

                        return;
                    }

                    rides.computeIfPresent(
                            rideId,
                            (id, state) ->
                                    new RideChannelState(
                                            state.subscribers(),
                                            RideChannelState.Status.SUBSCRIBED
                                    )
                    );

                    log.info("Subscribed to {}", channel);
                });
    }

    private void unsubscribeRedis(String rideId) {

        String channel =
                RideChannelNames.redisChannel(rideId);

        connection.async()
                .sunsubscribe(channel)
                .whenComplete((result, ex) -> {

                    if (ex != null) {
                        log.error(
                                "Failed to unsubscribe from {}",
                                channel,
                                ex
                        );
                        return;
                    }

                    log.info(
                            "Unsubscribed from {}",
                            channel
                    );
                });
    }
}
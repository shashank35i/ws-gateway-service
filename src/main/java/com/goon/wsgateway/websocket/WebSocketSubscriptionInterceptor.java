package com.goon.wsgateway.websocket;

import com.goon.wsgateway.redis.RideChannelManager;
import com.goon.wsgateway.redis.RideChannelNames;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class WebSocketSubscriptionInterceptor
        implements ChannelInterceptor {

    private final RideChannelManager rideChannelManager;

    private final Map<String, Map<String, String>> subscriptions =
            new ConcurrentHashMap<>();

    @Override
    public Message<?> preSend(
            Message<?> message,
            MessageChannel channel
    ) {

        StompHeaderAccessor accessor =
                StompHeaderAccessor.wrap(message);

        StompCommand command =
                accessor.getCommand();

        if (command == null) {
            return message;
        }

        switch (command) {

            case SUBSCRIBE ->
                    handleSubscribe(accessor);

            case UNSUBSCRIBE ->
                    handleUnsubscribe(accessor);

            case DISCONNECT ->
                    handleDisconnect(accessor);

            default -> {
            }
        }

        return message;
    }

    private void handleSubscribe(
            StompHeaderAccessor accessor
    ) {

        String destination =
                accessor.getDestination();

        String sessionId =
                accessor.getSessionId();

        String subscriptionId =
                accessor.getSubscriptionId();

        if (destination == null
                || sessionId == null
                || subscriptionId == null) {
            return;
        }

        String rideId =
                RideChannelNames
                        .extractRideIdFromWebSocketTopic(destination);

        if (rideId == null) {
            return;
        }

        Map<String, String> sessionSubscriptions =
                subscriptions.computeIfAbsent(
                        sessionId,
                        id -> new ConcurrentHashMap<>()
                );

        String previous =
                sessionSubscriptions.putIfAbsent(
                        subscriptionId,
                        rideId
                );

        if (previous == null) {
            rideChannelManager.subscribe(rideId);
        }
    }

    private void handleUnsubscribe(
            StompHeaderAccessor accessor
    ) {

        String sessionId =
                accessor.getSessionId();

        String subscriptionId =
                accessor.getSubscriptionId();

        if (sessionId == null
                || subscriptionId == null) {
            return;
        }

        Map<String, String> sessionSubscriptions =
                subscriptions.get(sessionId);

        if (sessionSubscriptions == null) {
            return;
        }

        String rideId =
                sessionSubscriptions.remove(subscriptionId);

        if (rideId != null) {
            rideChannelManager.unsubscribe(rideId);
        }

        if (sessionSubscriptions.isEmpty()) {
            subscriptions.remove(
                    sessionId,
                    sessionSubscriptions
            );
        }
    }

    private void handleDisconnect(
            StompHeaderAccessor accessor
    ) {

        String sessionId =
                accessor.getSessionId();

        if (sessionId == null) {
            return;
        }

        Map<String, String> sessionSubscriptions =
                subscriptions.remove(sessionId);

        if (sessionSubscriptions == null) {
            return;
        }

        sessionSubscriptions
                .values()
                .forEach(
                        rideChannelManager::unsubscribe
                );
    }
}
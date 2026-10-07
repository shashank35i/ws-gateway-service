package com.goon.wsgateway.websocket;

import com.goon.wsgateway.redis.RideChannelNames;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RideMessageForwarder {

    private final SimpMessagingTemplate messagingTemplate;

    public void forward(String rideId, String payload) {
        messagingTemplate.convertAndSend(
                RideChannelNames.websocketTopic(rideId),
                payload
        );
    }
}
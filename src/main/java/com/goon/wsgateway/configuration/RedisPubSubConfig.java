package com.goon.wsgateway.configuration;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisPubSubConfig {

    @Bean(destroyMethod = "shutdown")
    public RedisClient redisClient(
            @Value("${app.redis.uri}") String redisUri
    ) {
        return RedisClient.create(redisUri);
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisPubSubConnection<String, String> redisPubSubConnection(
            RedisClient redisClient
    ) {
        return redisClient.connectPubSub();
    }
}
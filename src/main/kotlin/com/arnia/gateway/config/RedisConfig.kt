package com.arnia.gateway.config

import com.arnia.gateway.service.PublicChannelSubscriber
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.listener.PatternTopic
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer

@Configuration
class RedisConfig {
    @Bean
    fun publicChannelListener(
        connectionFactory: ReactiveRedisConnectionFactory,
        publicChannelSubscriber: PublicChannelSubscriber,
    ): ReactiveRedisMessageListenerContainer {
        val container = ReactiveRedisMessageListenerContainer(connectionFactory)
        container
            .receive(PatternTopic.of("/topic/game/*"))
            .doOnNext(publicChannelSubscriber::onMessage)
            .doOnError(publicChannelSubscriber::onError)
            .subscribe()
        return container
    }
}

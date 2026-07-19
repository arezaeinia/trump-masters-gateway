package com.arnia.gateway.config

import com.redis.testcontainers.RedisContainer
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.RedisStandaloneConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@TestConfiguration
@Testcontainers
class RedisTestContainerConfig {
    companion object {
        @Container
        val REDIS_CONTAINER: RedisContainer =
            RedisContainer(
                DockerImageName.parse("redis:7.0-alpine"),
            ).also { it.start() }

        @JvmStatic
        @DynamicPropertySource
        fun configureRedisProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.redis.host", REDIS_CONTAINER::getHost)
            registry.add("spring.data.redis.port") { REDIS_CONTAINER.getMappedPort(6379) }
            registry.add("spring.data.redis.password") { "" }
        }
    }

    @Bean
    @Primary
    fun testRedisConnectionFactory(): RedisConnectionFactory {
        val config =
            RedisStandaloneConfiguration(
                REDIS_CONTAINER.host,
                REDIS_CONTAINER.firstMappedPort,
            )
        return LettuceConnectionFactory(config).also { it.afterPropertiesSet() }
    }
}

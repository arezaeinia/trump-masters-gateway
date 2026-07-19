package com.arnia.gateway.config

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import java.time.Duration

@Configuration
class WebClientConfig(
    @Value("\${backend.base-url}") private val backendBaseUrl: String,
    @Value("\${backend.connection-pool.max-connections}") private val maxConnections: Int,
    @Value("\${backend.connection-pool.pending-acquire-max}") private val pendingAcquireMax: Int,
    @Value("\${backend.connection-pool.max-idle-time-seconds}") private val maxIdleTimeSeconds: Long,
    @Value("\${backend.timeout-seconds}") private val timeoutSeconds: Long,
) {
    @Autowired(required = false)
    private var bearerTokenFilter: BearerTokenExchangeFilter? = null

    @Bean
    fun webClient(): WebClient {
        val provider =
            ConnectionProvider
                .builder("backend-pool")
                .maxConnections(maxConnections)
                .pendingAcquireMaxCount(pendingAcquireMax)
                .maxIdleTime(Duration.ofSeconds(maxIdleTimeSeconds))
                .build()

        val httpClient =
            HttpClient
                .create(provider)
                .responseTimeout(Duration.ofSeconds(timeoutSeconds))

        val builder =
            WebClient
                .builder()
                .baseUrl(backendBaseUrl)
                .clientConnector(ReactorClientHttpConnector(httpClient))

        // Conditionally add Bearer token filter (only when service.token.enabled=true)
        bearerTokenFilter?.let { filter ->
            builder.filter(filter)
        }

        return builder.build()
    }
}

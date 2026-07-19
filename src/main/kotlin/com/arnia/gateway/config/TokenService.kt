package com.arnia.gateway.config

import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference

/**
 * Service-to-service authentication token provider.
 * Fetches and caches OAuth2 tokens from Keycloak for backend API calls.
 * Only active when service.token.enabled=true (disabled in load-test environments).
 */
@Component
@ConditionalOnProperty(
    prefix = "service.token",
    name = ["enabled"],
    havingValue = "true",
    matchIfMissing = false,
)
class TokenService {
    private val log = LoggerFactory.getLogger(javaClass)
    private val tokenCache = AtomicReference<String?>(null)

    @Value("\${service.token.username}")
    private lateinit var username: String

    @Value("\${service.token.password}")
    private lateinit var password: String

    @Value("\${service.token.client}")
    private lateinit var clientId: String

    @Value("\${service.token.keycloak-url}")
    private lateinit var keycloakUrl: String

    private lateinit var tokenWebClient: WebClient

    @PostConstruct
    fun init() {
        // Dedicated WebClient for Keycloak token endpoint (no auth needed)
        tokenWebClient =
            WebClient
                .builder()
                .baseUrl(keycloakUrl)
                .build()
        log.info("TokenService initialized with Keycloak URL: {}", keycloakUrl)
    }

    /**
     * Get cached access token or fetch a new one if not available.
     */
    fun getAccessToken(): Mono<String> {
        val cached = tokenCache.get()
        if (cached != null) {
            return Mono.just(cached)
        }
        return fetchNewToken()
    }

    /**
     * Force token refresh (called on 401 Unauthorized).
     */
    fun refreshToken(): Mono<String> {
        log.info("Refreshing access token")
        tokenCache.set(null)
        return fetchNewToken()
    }

    private fun fetchNewToken(): Mono<String> {
        val formData =
            LinkedMultiValueMap<String, String>().apply {
                add("username", username)
                add("password", password)
                add("client_id", clientId)
                add("grant_type", "password")
            }

        return tokenWebClient
            .post()
            .uri("/protocol/openid-connect/token")
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .body(BodyInserters.fromFormData(formData))
            .retrieve()
            .bodyToMono(LoginData::class.java)
            .doOnNext { loginData ->
                tokenCache.set(loginData.accessToken)
                log.debug("Access token fetched successfully (expires in: {}s)", loginData.expiresIn)
            }.map { it.accessToken }
            .doOnError { error ->
                log.error("Failed to fetch access token from Keycloak: {}", error.message, error)
            }
    }
}

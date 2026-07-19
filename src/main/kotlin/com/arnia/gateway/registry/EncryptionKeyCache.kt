package com.arnia.gateway.registry

import com.arnia.gateway.crypto.TripleDes
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap

/**
 * Fetches and caches [TripleDes] instances keyed by gameId.
 *
 * On the first message for a game the gateway calls GET /api/games/{id},
 * which returns [encryption_key] in the GameRunDto response.
 * Subsequent messages use the cached cipher — no extra HTTP round-trip.
 */
@Component
class EncryptionKeyCache(
    private val webClient: WebClient,
    @Value("\${backend.game-path:/api/games}") private val gamePath: String,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val objectMapper = ObjectMapper()
    private val cache = ConcurrentHashMap<Int, TripleDes>()

    fun get(
        gameId: Int,
        authHeader: String?,
    ): Mono<TripleDes> =
        cache[gameId]?.let { Mono.just(it) }
            ?: fetchEncryptionKey(gameId, authHeader)
                .map { base64Key ->
                    TripleDes.fromBase64Key(base64Key).also {
                        cache[gameId] = it
                        log.debug("Cached encryption key for gameId={}", gameId)
                    }
                }

    fun evict(gameId: Int) = cache.remove(gameId)

    private fun fetchEncryptionKey(
        gameId: Int,
        authHeader: String?,
    ): Mono<String> {
        val request = webClient.get().uri("$gamePath/{id}", gameId)
        if (authHeader != null) request.header(HttpHeaders.AUTHORIZATION, authHeader)
        return request
            .retrieve()
            .bodyToMono(String::class.java)
            .map { body ->
                val keyNode =
                    objectMapper.readTree(body).get("encryption_key")
                        ?: throw IllegalStateException("encryption_key missing for gameId=$gameId")
                keyNode.asText()
            }.doOnError { log.error("Failed to fetch encryption key for gameId={}: {}", gameId, it.message) }
    }
}

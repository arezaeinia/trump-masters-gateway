package com.arnia.gateway.service

import com.arnia.gateway.model.WebSocketMessage
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

/**
 * Central processor for game commands from WebSocket or Private Channel.
 * Routes commands based on type:
 * - Pass-through: Publish directly to Public Channel (fast path)
 * - Actionable: Call backend, merge result, publish to Public Channel
 */
@Service
class GameCommandProcessor(
    private val webClient: WebClient,
    private val redisTemplate: RedisTemplate<String, String>,
    private val mapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        // Pass-through commands (no backend processing needed)
        private val PASS_THROUGH_COMMANDS =
            setOf(
                "SEND_EMOJI",
                "SEND_CHAT",
                "MOVE_PROGRESS",
            )
    }

    /**
     * Processes incoming commands received over WebSocket.
     *
     * All responses are ultimately published to the public Redis channel, where
     * the PublicChannelSubscriber encrypts and delivers them to connected clients.
     *
     * Supported command types:
     *
     * 1. Pass-through commands
     *    Examples: chat messages, emojis, and other non-actionable events.
     *    These commands are published directly to the public channel without
     *    backend processing.
     *
     * 2. Actionable commands
     *    Examples: player moves and other game-state-changing actions.
     *    These commands are first sent to the backend for validation and processing.
     *    The backend response is then merged into a GameMessage and published to
     *    the public channel.
     *
     * This design ensures a single outbound delivery path for all real-time
     * client updates, regardless of command type.
     */
    fun processCommand(
        msg: WebSocketMessage,
        authHeader: String?,
    ): Mono<Void> {
        log.debug(
            "Processing command: type={}, gameId={}, sender={}",
            msg.type,
            msg.gameId,
            msg.sender,
        )

        return when {
            PASS_THROUGH_COMMANDS.contains(msg.type) -> {
                // Pass-through: Publish to Public Channel (preserves all fields)
                publishToPublicChannel(msg)
                Mono.empty()
            }
            else -> {
                // Actionable: Call backend
                callBackendAndPublish(msg, authHeader)
            }
        }
    }

    private fun callBackendAndPublish(
        msg: WebSocketMessage,
        authHeader: String?,
    ): Mono<Void> {
        val gameId = msg.gameId
        val commandRequest = buildCommandRequest(msg)

        return post("/api/games/$gameId/commands", commandRequest, authHeader, true)
            .flatMap { response ->
                // Parse backend response as generic map (avoid coupling to GameRunDto)
                val gameRunDto =
                    runCatching {
                        mapper.readValue(response, Map::class.java)
                    }.getOrElse { emptyMap<String, Any>() }

                // Merge backend result into original message (preserve all client-expected fields)
                val enrichedMessage = msg.copy(gameRunDto = gameRunDto)

                // Publish complete message to PUBLIC CHANNEL
                publishToPublicChannel(enrichedMessage)

                Mono.empty<Void>()
            }.doOnError { e ->
                log.error("Backend call failed for gameId={}: {}", gameId, e.message, e)
            }.onErrorResume { Mono.empty() }
    }

    private fun publishToPublicChannel(msg: WebSocketMessage) {
        try {
            val json = mapper.writeValueAsString(msg)

            // Publish to PUBLIC CHANNEL (gateway → all gateways)
            val publicChannel = "/topic/public/${msg.gameId}"
            redisTemplate.convertAndSend(publicChannel, json)

            log.debug(
                "Published to PUBLIC channel: type={}, gameId={}, sender={}",
                msg.type,
                msg.gameId,
                msg.sender,
            )
        } catch (e: Exception) {
            log.error("Failed to publish to public channel: {}", e.message, e)
        }
    }

    private fun buildCommandRequest(msg: WebSocketMessage): String {
        val moveIndex = msg.moveDto?.moveIndex ?: 0
        val indices = msg.moveDto?.indices ?: intArrayOf()
        val cellIndex = msg.clientRequest?.cellIndex ?: 0

        val request =
            mapOf(
                "type" to msg.type,
                "userId" to msg.senderUserId,
                "moveIndex" to moveIndex,
                "indices" to indices,
                "cellIndex" to cellIndex,
            )
        return mapper.writeValueAsString(request)
    }

    private fun post(
        path: String,
        jsonBody: String?,
        authHeader: String?,
        isGatewayRequest: Boolean = false,
    ): Mono<String> =
        webClient
            .post()
            .uri(path)
            .apply {
                if (authHeader != null) header(HttpHeaders.AUTHORIZATION, authHeader)
                if (isGatewayRequest) header("X-Gateway-Request", "true")
            }.contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonBody ?: "{}")
            .retrieve()
            .bodyToMono(String::class.java)
            .onErrorResume { e ->
                log.warn("Backend call failed [{}]: {}", path, e.message)
                Mono.error(e)
            }
}

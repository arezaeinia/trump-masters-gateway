package com.arnia.gateway.service

import com.arnia.gateway.model.OuterMessage
import com.arnia.gateway.model.WebSocketMessage
import com.arnia.gateway.registry.EncryptionKeyCache
import com.arnia.gateway.registry.SessionRegistry
import com.arnia.gateway.stomp.StompParser
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.connection.ReactiveSubscription
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

/**
 * Handles outbound real-time messages delivered through Redis Pub/Sub.
 *
 * This component receives messages published to Redis, encrypts the payload,
 * and forwards it to the appropriate client over WebSocket. Since users may be
 * connected to any gateway instance, each instance determines whether the target
 * client is connected locally. If so, the message is delivered; otherwise, it is ignored.
 *
 * Messages are typically published by backend services to channels (server initiated message like robot-actions or timeouts)
 * or, the gateway itself invoke backend APIs, receive the response,
 * and then publish the resulting message to Redis for distribution.
 */
@Component
class PublicChannelSubscriber(
    private val sessionRegistry: SessionRegistry,
    private val encryptionKeyCache: EncryptionKeyCache,
    private val mapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun onMessage(message: ReactiveSubscription.Message<String, String>) {
        try {
            val channel = message.channel
            val gameId =
                channel.substringAfterLast("/").toIntOrNull() ?: run {
                    log.warn("Could not extract gameId from Redis channel: {}", channel)
                    return
                }

            val gameMessage = mapper.readValue(message.message, WebSocketMessage::class.java)

            log.debug(
                "Received from PUBLIC channel: type={}, gameId={}, sender={}",
                gameMessage.type,
                gameId,
                gameMessage.sender,
            )

            val entries = sessionRegistry.getEntries(gameId)
            if (entries.isEmpty()) {
                log.debug("No active sessions for gameId={}, dropping message", gameId)
                return
            }

            encryptionKeyCache
                .get(gameId, null)
                .flatMap { td ->
                    val plainJson = mapper.writeValueAsString(gameMessage)
                    val encrypted =
                        td.encrypt(plainJson) ?: run {
                            log.error("Failed to encrypt message for gameId={}", gameId)
                            return@flatMap Mono.empty<Void>()
                        }

                    // Wrap encrypted data in the format client expects: { gameId, encryptedData }
                    val messageBody =
                        OuterMessage(
                            gameId,
                            encrypted,
                        )
                    val body = mapper.writeValueAsString(messageBody)

                    log.debug("Broadcasting encrypted message to {} session(s) for gameId={}", entries.size, gameId)

                    entries.forEach { (session, subscriptionId) ->
                        if (session.isOpen) {
                            val stompFrame =
                                StompParser.message(
                                    subscriptionId = subscriptionId,
                                    destination = channel,
                                    body = body,
                                )
                            // Send raw STOMP frame (no SockJS wrapping)
                            val wireFrame = StompParser.serialise(stompFrame)

                            session
                                .send(Mono.just(session.textMessage(wireFrame)))
                                .doOnError { log.warn("Failed to push to session {}: {}", session.id, it.message) }
                                .onErrorResume { Mono.empty() }
                                .subscribe()
                        } else {
                            sessionRegistry.deregister(gameId, session)
                        }
                    }

                    Mono.empty<Void>()
                }.doOnError { e ->
                    log.error("Failed to encrypt/broadcast for gameId={}: {}", gameId, e.message, e)
                }.subscribe()
        } catch (e: Exception) {
            log.error("Error processing public channel message: {}", e.message, e)
        }
    }

    fun onError(error: Throwable) {
        log.error("Redis public channel subscription error: {}", error.message, error)
    }
}

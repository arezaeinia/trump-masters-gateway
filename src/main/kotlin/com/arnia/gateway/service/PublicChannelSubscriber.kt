package com.arnia.gateway.service

import com.arnia.gateway.registry.SessionRegistry
import com.arnia.gateway.stomp.StompParser
import org.slf4j.LoggerFactory
import org.springframework.data.redis.connection.ReactiveSubscription
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

/**
 * Fans out backend broadcasts to connected clients. The backend publishes `EVENT` envelopes (plain
 * JSON) to Redis `/topic/game/{gameId}`; this receives them and forwards the body **as-is** to every
 * local session subscribed to that game, wrapped in a STOMP `MESSAGE` frame. No decryption, no
 * re-serialisation, no state merge — the gateway is a pure transport for the backend's authoritative
 * events (publish-ownership, protocol §11).
 */
@Component
class PublicChannelSubscriber(
    private val sessionRegistry: SessionRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun onMessage(message: ReactiveSubscription.Message<String, String>) {
        val channel = message.channel
        val gameId =
            channel.substringAfterLast('/').toIntOrNull() ?: run {
                log.warn("Could not extract gameId from Redis channel: {}", channel)
                return
            }

        val entries = sessionRegistry.getEntries(gameId)
        if (entries.isEmpty()) {
            log.debug("No active sessions for gameId={}, dropping event", gameId)
            return
        }

        val body = message.message
        log.debug("Broadcasting event to {} session(s) for gameId={}", entries.size, gameId)

        entries.forEach { (session, subscriptionId) ->
            if (session.isOpen) {
                val frame = StompParser.message(subscriptionId = subscriptionId, destination = channel, body = body)
                session
                    .send(Mono.just(session.textMessage(StompParser.serialise(frame))))
                    .doOnError { log.warn("Failed to push to session {}: {}", session.id, it.message) }
                    .onErrorResume { Mono.empty() }
                    .subscribe()
            } else {
                sessionRegistry.deregister(gameId, session)
            }
        }
    }

    fun onError(error: Throwable) {
        log.error("Redis game-channel subscription error: {}", error.message, error)
    }
}

package com.arnia.gateway.registry

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketSession
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

data class SessionEntry(
    val session: WebSocketSession,
    /** STOMP subscription id from the client's SUBSCRIBE frame (e.g. "sub-0"). */
    val subscriptionId: String,
)

/**
 * Tracks which WebSocket sessions are subscribed to which gameId.
 *
 * Populated when the client sends a STOMP SUBSCRIBE frame for
 * `/topic/game/{gameId}`. Used by [com.arnia.gateway.service.PublicChannelSubscriber]
 * to push async backend events to the correct connections with the correct
 * STOMP subscription id in the MESSAGE frame, and by the handler to address a private
 * `COMMAND_ERROR` back to a commanding session (see [subscriptionIdFor]).
 */
@Component
class SessionRegistry {
    private val log = LoggerFactory.getLogger(javaClass)
    private val registry = ConcurrentHashMap<Int, CopyOnWriteArrayList<SessionEntry>>()

    /** Reverse index: sessionId → its subscription id, for addressing private frames back to a session. */
    private val subscriptionBySession = ConcurrentHashMap<String, String>()

    fun register(
        gameId: Int,
        session: WebSocketSession,
        subscriptionId: String,
    ) {
        val entries = registry.computeIfAbsent(gameId) { CopyOnWriteArrayList() }

        // Prevent duplicate registrations for the same session + subscription
        // (e.g., if client sends SUBSCRIBE twice due to network retry)
        entries.removeIf { it.session.id == session.id && it.subscriptionId == subscriptionId }

        entries.add(SessionEntry(session, subscriptionId))
        subscriptionBySession[session.id] = subscriptionId
        log.debug("Session registered: gameId={}, sessionId={}, subId={}", gameId, session.id, subscriptionId)
    }

    fun deregister(
        gameId: Int,
        session: WebSocketSession,
    ) {
        registry[gameId]?.let { entries ->
            entries.removeIf { it.session.id == session.id }
            if (entries.isEmpty()) registry.remove(gameId)
        }
        subscriptionBySession.remove(session.id)
        log.debug("Session deregistered: gameId={}, sessionId={}", gameId, session.id)
    }

    fun getEntries(gameId: Int): List<SessionEntry> = registry.getOrDefault(gameId, CopyOnWriteArrayList())

    /** The STOMP subscription id this session subscribed with, or null if it never subscribed. */
    fun subscriptionIdFor(session: WebSocketSession): String? = subscriptionBySession[session.id]
}

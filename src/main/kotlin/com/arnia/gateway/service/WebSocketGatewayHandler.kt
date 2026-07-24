package com.arnia.gateway.service

import com.arnia.gateway.model.Command
import com.arnia.gateway.model.CommandError
import com.arnia.gateway.registry.SessionRegistry
import com.arnia.gateway.stomp.StompParser
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks

/**
 * Handles the client ↔ gateway protocol over STOMP-on-raw-WebSocket (WebFlux). Plain JSON, no
 * encryption (protocol §3, §8). Per connection:
 *
 *   1. `CONNECT`   → reply `CONNECTED`, capture the bearer token.
 *   2. `SUBSCRIBE` → register the session under its gameId in [SessionRegistry].
 *   3. `SEND`      → parse the body as a [Command], route it to the backend REST ([CommandRouter]).
 *                    On failure, push a private `COMMAND_ERROR` back as a STOMP `MESSAGE` frame
 *                    (never a STOMP `ERROR`, which would drop the socket). On success, nothing — the
 *                    backend publishes the resulting broadcast, which arrives via Redis fan-out.
 *   4. Server events arrive out-of-band through [PublicChannelSubscriber] as `MESSAGE` frames.
 */
@Component
class WebSocketGatewayHandler(
    private val sessionRegistry: SessionRegistry,
    private val commandRouter: CommandRouter,
    private val mapper: ObjectMapper,
) : WebSocketHandler {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun handle(session: WebSocketSession): Mono<Void> {
        val sink = Sinks.many().unicast().onBackpressureBuffer<String>()

        val inbound =
            session
                .receive()
                .map { it.payloadAsText }
                .flatMap { raw -> handleFrame(raw, session, sink) }
                .doOnError { log.error("Inbound error session={}: {}", session.id, it.message) }
                .then()

        val outbound = session.send(sink.asFlux().map { session.textMessage(it) })

        return Mono
            .zip(inbound, outbound)
            .doFinally {
                log.debug("Session closed: id={}, signal={}", session.id, it)
                sink.tryEmitComplete()
            }.then()
    }

    private fun handleFrame(
        raw: String,
        session: WebSocketSession,
        sink: Sinks.Many<String>,
    ): Flux<Void> {
        val frame =
            runCatching { StompParser.parse(raw) }.getOrElse {
                log.warn("Failed to parse STOMP frame: {}", it.message)
                return Flux.empty()
            }

        val result: Mono<Void> =
            when (frame.command) {
                "CONNECT" -> {
                    sink.tryEmitNext(StompParser.serialise(StompParser.connected()))
                    Mono.empty()
                }

                "SUBSCRIBE" -> {
                    val destination = frame.header("destination")
                    val subscriptionId = frame.header("id") ?: "sub-0"
                    val gameId = destination?.let(::gameIdFromTopic)
                    if (gameId != null) sessionRegistry.register(gameId, session, subscriptionId)
                    Mono.empty()
                }

                "SEND" -> routeSend(frame.body, session, sink)

                "DISCONNECT" -> {
                    sink.tryEmitComplete()
                    Mono.empty()
                }

                else -> Mono.empty()
            }

        return Flux.from(result)
    }

    /** Parse the SEND body as a [Command], route it, and on failure push a private `COMMAND_ERROR`. */
    private fun routeSend(
        body: String,
        session: WebSocketSession,
        sink: Sinks.Many<String>,
    ): Mono<Void> {
        val command =
            runCatching { mapper.readValue(body, Command::class.java) }.getOrNull()
        if (command?.type == null || command.gameId <= 0) {
            pushError(sink, session, CommandError(command?.correlationId, command?.type, "malformed-message", 400, "malformed command"))
            return Mono.empty()
        }

        val authHeader = session.handshakeInfo.headers.getFirst(HttpHeaders.AUTHORIZATION)
        return commandRouter
            .route(command, authHeader)
            .doOnNext { error -> pushError(sink, session, error) }
            .onErrorResume { ex ->
                log.warn("routeSend error type={}: {}", command.type, ex.message)
                pushError(
                    sink,
                    session,
                    CommandError(command.correlationId, command.type, "backend-unreachable", 502, ex.message ?: "error"),
                )
                Mono.empty()
            }.then()
    }

    /**
     * Deliver a [CommandError] privately to [session] as a STOMP `MESSAGE` frame (protocol §3.3, §7).
     * Only the commanding session receives it — it is written straight to that session's outbound sink,
     * never to Redis.
     */
    private fun pushError(
        sink: Sinks.Many<String>,
        session: WebSocketSession,
        error: CommandError,
    ) {
        val subscriptionId = sessionRegistry.subscriptionIdFor(session) ?: "sub-0"
        val json = mapper.writeValueAsString(error)
        val frame = StompParser.message(subscriptionId = subscriptionId, destination = PRIVATE_DESTINATION, body = json)
        sink.tryEmitNext(StompParser.serialise(frame))
    }

    /** Extract the gameId from a topic destination like `/topic/game/42`. */
    private fun gameIdFromTopic(destination: String): Int? = destination.substringAfterLast('/').toIntOrNull()

    private companion object {
        const val PRIVATE_DESTINATION = "/user/queue/errors"
    }
}

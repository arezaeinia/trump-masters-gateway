package com.arnia.gateway.service

import com.arnia.gateway.model.OuterMessage
import com.arnia.gateway.model.WebSocketMessage
import com.arnia.gateway.registry.EncryptionKeyCache
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
 * Handles raw STOMP protocol on top of WebFlux raw WebSocket.
 *
 * Wire flow per connection:
 *   1. Client sends `CONNECT\n...\n\n\u0000`
 *   2. Gateway replies `CONNECTED\n...\n\n\u0000`, extracts auth token
 *   3. Client sends `SUBSCRIBE\ndestination:/topic/public/{gameId}\nid:sub-0\n\n\u0000`
 *      → gateway registers session in SessionRegistry
 *   4. Client sends `SEND\ndestination:/app/game.sendMessage\n\n{encrypted}\u0000`
 *      → gateway decrypts, routes to backend REST, response comes back via Redis push
 *   5. Server-initiated events arrive via RedisSubscriber → pushed as STOMP MESSAGE frames
 */
@Component
class WebSocketGatewayHandler(
    private val sessionRegistry: SessionRegistry,
    private val keyCache: EncryptionKeyCache,
    private val commandProcessor: GameCommandProcessor,
) : WebSocketHandler {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()

    // -------------------------------------------------------------------------
    // Main entry point
    // -------------------------------------------------------------------------

    override fun handle(session: WebSocketSession): Mono<Void> {
        // Sink for server-initiated messages (CONNECTED reply, errors, etc.)
        val sink = Sinks.many().unicast().onBackpressureBuffer<String>()

        // Auth token extracted from STOMP CONNECT frame
        var authToken: String? = session.handshakeInfo.headers.getFirst(HttpHeaders.AUTHORIZATION)

        val inbound =
            session
                .receive()
                .map { it.payloadAsText }
                .flatMap { raw -> handleFrame(raw, session, sink) { authToken = it } }
                .doOnError { log.error("Inbound error session={}: {}", session.id, it.message) }
                .then()

        val outbound =
            session.send(
                sink.asFlux().map { session.textMessage(it) },
            )

        return Mono
            .zip(inbound, outbound)
            .doFinally {
                log.debug("Session closed: id={}, signal={}", session.id, it)
                sink.tryEmitComplete()
            }.then()
    }

    // -------------------------------------------------------------------------
    // Frame dispatcher
    // -------------------------------------------------------------------------

    private fun handleFrame(
        raw: String,
        session: WebSocketSession,
        sink: Sinks.Many<String>,
        onAuthToken: (String) -> Unit,
    ): Flux<Void> {
        log.debug("Raw frame received, length={}, preview={}", raw.length, raw.take(500).replace("\n", "\\n"))

        // Parse raw STOMP frame
        val frame =
            runCatching { StompParser.parse(raw) }.getOrElse {
                log.warn("Failed to parse STOMP frame: {}", it.message)
                return Flux.empty()
            }

        log.debug("Parsed STOMP frame: command={}, bodyLength={}", frame.command, frame.body.length)

        val result =
            when (frame.command) {
                "CONNECT" -> {
                    // Extract auth from STOMP headers
                    frame
                        .header("Authorization")
                        ?.takeIf { it.isNotBlank() }
                        ?.let { onAuthToken(it) }
                    // Also check login header (some clients put the token there)
                    frame
                        .header("login")
                        ?.takeIf { it.isNotBlank() }
                        ?.let { onAuthToken("Bearer $it") }

                    // Send raw STOMP CONNECTED frame
                    sink.tryEmitNext(StompParser.serialise(StompParser.connected()))
                    Mono.empty<Void>()
                }

                "SUBSCRIBE" -> {
                    val destination = frame.header("destination") ?: return Flux.empty()
                    val subscriptionId = frame.header("id") ?: "sub-0"
                    val gameId = extractGameIdFromTopic(destination)
                    if (gameId != null) {
                        sessionRegistry.register(gameId, session, subscriptionId)
                    }
                    Mono.empty<Void>()
                }

                "UNSUBSCRIBE" -> {
                    // Best-effort: we'd need reverse lookup; skip for now
                    Mono.empty<Void>()
                }

                "SEND" -> {
                    log.debug("Processing SEND frame, body length={}, body preview={}", frame.body.length, frame.body.take(100))
                    val authHeader = session.handshakeInfo.headers.getFirst(HttpHeaders.AUTHORIZATION)
                    routeSend(frame.body, authHeader, sink)
                }

                "DISCONNECT" -> {
                    sink.tryEmitComplete()
                    Mono.empty<Void>()
                }

                else -> {
                    log.debug("Unhandled STOMP command: {}", frame.command)
                    Mono.empty<Void>()
                }
            }

        return Flux.from(result)
    }

    // -------------------------------------------------------------------------
    // SEND routing — decrypt, call backend REST, response via Redis (fire-and-forget)
    // -------------------------------------------------------------------------

    private fun routeSend(
        body: String,
        authHeader: String?,
        sink: Sinks.Many<String>,
    ): Mono<Void> {
        // Parse outer envelope: { gameId, encryptedData }
        val (gameId, encryptedData) =
            parseOuter(body) ?: run {
                pushError(sink, "malformed message")
                log.debug("Failed to parse outer message envelope: body length={}, body preview={}", body.length, body.take(100))
                return Mono.empty()
            }

        return keyCache
            .get(gameId, authHeader)
            .flatMap { td ->
                val decrypted =
                    td.decrypt(encryptedData) ?: run {
                        pushError(sink, "decryption failed")
                        return@flatMap Mono.empty<Void>()
                    }

                val msg =
                    runCatching { mapper.readValue(decrypted, WebSocketMessage::class.java) }
                        .getOrElse {
                            pushError(sink, "invalid message format")
                            return@flatMap Mono.empty<Void>()
                        }

                log.debug("Routing type={} gameId={}", msg.type, gameId)

                commandProcessor
                    .processCommand(msg, authHeader)
                    .doOnError { pushError(sink, it.message) }
            }.onErrorResume { e ->
                log.warn("routeSend error gameId={}: {}", gameId, e.message)
                pushError(sink, e.message)
                Mono.empty()
            }
    }

    private fun parseOuter(raw: String): OuterMessage? =
        runCatching {
            val node = mapper.readTree(raw)
            val gameId = node.path("gameId").asInt(-1)
            val encryptedData = node.path("encryptedData").asText(null)
            if (gameId <= 0 || encryptedData == null) return null
            OuterMessage(gameId, encryptedData)
        }.getOrNull()

    /** Extract gameId from STOMP topic destination "/topic/public/42" */
    private fun extractGameIdFromTopic(destination: String): Int? = destination.substringAfterLast("/").toIntOrNull()

    private fun pushError(
        sink: Sinks.Many<String>,
        message: String?,
    ) {
        val errorFrame = StompParser.error(message ?: "unknown error")
        sink.tryEmitNext(StompParser.serialise(errorFrame))
    }

    private fun sanitize(s: String?) = s?.replace("\"", "'")?.replace("\n", " ") ?: "unknown error"
}

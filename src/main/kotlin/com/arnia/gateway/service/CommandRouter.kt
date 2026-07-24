package com.arnia.gateway.service

import com.arnia.gateway.model.Command
import com.arnia.gateway.model.CommandError
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

/**
 * Routes a client [Command] to exactly one backend REST endpoint (protocol §4). The gateway inspects
 * the response **only** to decide success vs. failure — it does not build or publish an `EVENT`. On
 * success the backend publishes the resulting broadcast to Redis (publish-ownership, protocol §11);
 * on failure this returns a [CommandError] for the caller to deliver privately.
 *
 * `gameId` / `roundNumber` come verbatim from the client envelope; the backend rejects a stale
 * `roundNumber` as `invalid-round-state` (409). The bearer token is added by the WebClient's
 * exchange filter; `X-Gateway-Request: true` marks the call as gateway-originated.
 */
@Service
class CommandRouter(
    private val webClient: WebClient,
    private val mapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** Route [command]. Returns `Mono.empty()` on success, or a [CommandError] on failure. */
    fun route(
        command: Command,
        authHeader: String?,
    ): Mono<CommandError> {
        val type = command.type ?: return Mono.just(gatewayError(command, "unknown-command-type", "missing command type"))
        val route =
            ROUTES[type]
                ?: return Mono.just(gatewayError(command, "unknown-command-type", "unknown command type: $type"))

        val path = route.path(command)
        val body = route.body(command, mapper)

        return webClient
            .post()
            .uri(path)
            .apply {
                if (authHeader != null) header(HttpHeaders.AUTHORIZATION, authHeader)
                header(GATEWAY_REQUEST_HEADER, "true")
            }.contentType(MediaType.APPLICATION_JSON)
            .bodyValue(body)
            .retrieve()
            .toBodilessEntity()
            .then(Mono.empty<CommandError>())
            .onErrorResume(WebClientResponseException::class.java) { ex ->
                Mono.just(problemToError(command, ex))
            }.onErrorResume { ex ->
                log.warn("route {} failed (non-HTTP): {}", type, ex.message)
                Mono.just(gatewayError(command, "backend-unreachable", ex.message ?: "backend call failed"))
            }
    }

    /** Map a backend RFC 9457 Problem Detail (application/problem+json) to a [CommandError]. */
    private fun problemToError(
        command: Command,
        ex: WebClientResponseException,
    ): CommandError {
        val status = ex.statusCode.value()
        val problem = runCatching { mapper.readTree(ex.responseBodyAsString) }.getOrNull()
        // Problem `type` is a URI like ".../errors/not-player-turn"; the slug is the last segment.
        val code =
            problem
                ?.path("type")
                ?.asText(null)
                ?.substringAfterLast('/')
                ?.takeIf { it.isNotBlank() }
                ?: "backend-error"
        val message =
            problem?.path("detail")?.asText(null)?.takeIf { it.isNotBlank() }
                ?: problem?.path("title")?.asText(null)
                ?: ex.statusText
        log.debug("Command {} -> {} ({}): {}", command.type, code, status, message)
        return CommandError(command.correlationId, command.type, code, status, message)
    }

    private fun gatewayError(
        command: Command,
        code: String,
        message: String,
    ) = CommandError(command.correlationId, command.type, code, GATEWAY_ERROR_STATUS, message)

    /** A single command→REST binding: how to build the path and the request body from a [Command]. */
    private data class Route(
        val path: (Command) -> String,
        val body: (Command, ObjectMapper) -> JsonNode,
    )

    private companion object {
        const val GATEWAY_REQUEST_HEADER = "X-Gateway-Request"
        const val GATEWAY_ERROR_STATUS = 400

        /** Copy the named fields from the command's payload into a fresh JSON object for the REST body. */
        fun bodyFrom(
            command: Command,
            mapper: ObjectMapper,
            vararg fields: String,
        ): ObjectNode {
            val out = mapper.createObjectNode()
            val payload = command.payload
            if (payload != null) {
                for (f in fields) {
                    if (payload.has(f)) out.set<JsonNode>(f, payload.get(f))
                }
            }
            return out
        }

        val ROUTES: Map<String, Route> =
            mapOf(
                "DECLARE_TRUMP" to
                    Route(
                        path = { "/api/games/${it.gameId}/rounds/${it.roundNumber}/trump" },
                        body = { c, m -> bodyFrom(c, m, "userId", "trumpSuit") },
                    ),
                "PLAY_CARD" to
                    Route(
                        path = { "/api/games/${it.gameId}/rounds/${it.roundNumber}/plays" },
                        body = { c, m -> bodyFrom(c, m, "userId", "card") },
                    ),
                "LEAVE_GAME" to
                    Route(
                        // userId goes on the query string for the leave endpoint; the body is empty.
                        path = { "/api/games/${it.gameId}/leave?userId=${it.payload?.path("userId")?.asLong() ?: 0}" },
                        body = { _, m -> m.createObjectNode() },
                    ),
            )
    }
}

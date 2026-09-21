package com.arnia.gateway.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.JsonNode

/**
 * The client → gateway command envelope (protocol §3.1). Plain JSON in the STOMP `SEND` body — no
 * encryption, no outer `{gameId, encryptedData}` wrapper.
 *
 * ```json
 * { "kind":"COMMAND", "correlationId":"c-42", "type":"PLAY_CARD",
 *   "gameId":7, "roundNumber":3, "payload":{ "userId":31, "card":"HEARTS_10" } }
 * ```
 *
 * [payload] is left as a raw [JsonNode] — the router copies it (or fields of it) into the backend REST
 * body without the gateway needing to know each command's shape.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class Command(
    val kind: String? = null,
    val correlationId: String? = null,
    val type: String? = null,
    val gameId: Int = 0,
    val roundNumber: Int? = null,
    val payload: JsonNode? = null,
)

/**
 * The gateway → commanding-session error envelope (protocol §3.3). Delivered privately to only the
 * session that sent the failed command, as a STOMP `MESSAGE` frame (never a STOMP `ERROR` frame,
 * which would drop the socket). Echoes [correlationId] and [type] so the client can match it.
 */
data class CommandError(
    val correlationId: String?,
    val type: String?,
    val code: String,
    val status: Int,
    val message: String,
    val kind: String = "COMMAND_ERROR",
)

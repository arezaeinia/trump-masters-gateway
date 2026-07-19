package com.arnia.gateway.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Complete representation of a game message for the two-channel architecture.
 * Contains ALL fields needed for both actionable commands and pass-through messages.
 * Public Channel messages include gameRunDto from backend responses.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class WebSocketMessage(
    val type: String? = null,
    val gameId: Int = 0,
    val senderUserId: Int = 0,
    val sender: String? = null, // Display name of sender (for chat/emoji)
    val content: String? = null, // Content for SEND_CHAT, SEND_EMOJI, etc.
    // Present for v1 messages: MOVE_CONFIRM, START_TIMER, RESET_TIME, TIME_OUT, etc.
    val moveDto: MoveData? = null,
    /** Present for power-up messages: CHANGE_ALPHABET, BOMB */
    val clientRequest: ClientData? = null,
    /** Game state from backend (present in Public Channel messages after backend processing) */
    val gameRunDto: Any? = null, // Any to avoid coupling to backend's GameRunDto
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class MoveData(
        val indices: IntArray? = null,
        val moveIndex: Int = 0,
        val ai: Boolean = false,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class ClientData(
        val cellIndex: Int = 0,
        val moveIndex: Int = 0,
        val gameId: Int = 0,
    )
}

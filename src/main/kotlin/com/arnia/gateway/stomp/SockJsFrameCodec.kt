package com.arnia.gateway.stomp

import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Codec for the SockJS framing layer that wraps STOMP frames on the wire.
 *
 * SockJS frame types:
 *   o          — open (sent by server on new connection)
 *   h          — heartbeat (sent by server periodically)
 *   a["..."]   — array of messages (each is a STOMP frame string)
 *   c[code,"reason"] — close
 */
object SockJsFrameCodec {
    private val mapper = ObjectMapper()

    const val OPEN = "o"
    const val HEARTBEAT = "h"

    /**
     * Unwrap a SockJS `a[...]` frame into the list of STOMP frame strings inside.
     * Returns null for heartbeat (`h`) and open (`o`) frames — caller handles those.
     */
    fun unwrap(sockJsFrame: String): List<String>? {
        val trimmed = sockJsFrame.trim()
        return when {
            trimmed == OPEN || trimmed == HEARTBEAT -> null
            trimmed.startsWith("a") -> {
                // a["STOMP_FRAME"] — JSON array of strings
                val arrayJson = trimmed.substring(1)
                val arr = mapper.readValue(arrayJson, Array<String>::class.java)
                arr.toList()
            }
            else -> null
        }
    }

    /**
     * Wrap a STOMP frame string into a SockJS `a[...]` message frame.
     */
    fun wrap(stompText: String): String {
        // Escape the STOMP frame as a JSON string inside a JSON array
        return "a" + mapper.writeValueAsString(arrayOf(stompText))
    }
}

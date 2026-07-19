package com.arnia.gateway.stomp

data class StompFrame(
    val command: String,
    val headers: Map<String, String> = emptyMap(),
    val body: String = "",
) {
    fun header(name: String): String? = headers[name]
}

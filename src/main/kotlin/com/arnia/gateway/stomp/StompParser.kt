package com.arnia.gateway.stomp

/**
 * Parses and serialises STOMP 1.1 frames.
 *
 * Wire format:
 *   COMMAND\n
 *   header1:value1\n
 *   header2:value2\n
 *   \n
 *   body\u0000
 */
object StompParser {
    private const val NULL = "\u0000"
    private const val LF = "\n"

    /** Parse a raw STOMP text frame into a [StompFrame]. */
    fun parse(raw: String): StompFrame {
        // Strip trailing null byte and whitespace
        val text = raw.trimEnd('\u0000', '\n', '\r')

        // Split on the blank line that separates headers from body
        val blankLine = text.indexOf("$LF$LF")
        val headerSection: String
        val body: String

        if (blankLine >= 0) {
            headerSection = text.substring(0, blankLine)
            body = text.substring(blankLine + 2)
        } else {
            headerSection = text
            body = ""
        }

        val lines = headerSection.split(LF)
        val command = lines.first().trim()
        val headers =
            lines
                .drop(1)
                .filter { it.contains(':') }
                .associate { line ->
                    val colon = line.indexOf(':')
                    line.substring(0, colon).trim() to line.substring(colon + 1).trim()
                }

        return StompFrame(command, headers, body)
    }

    /** Serialise a [StompFrame] back to STOMP wire format. */
    fun serialise(frame: StompFrame): String =
        buildString {
            append(frame.command)
            append(LF)
            frame.headers.forEach { (k, v) ->
                append(k)
                append(':')
                append(v)
                append(LF)
            }
            append(LF)
            append(frame.body)
            append(NULL)
        }

    // ── Convenience constructors for server-sent frames ──────────────────────

    fun connected(): StompFrame =
        StompFrame(
            command = "CONNECTED",
            headers =
                mapOf(
                    "version" to "1.1",
                    "heart-beat" to "0,0",
                ),
        )

    fun message(
        subscriptionId: String,
        destination: String,
        body: String,
    ): StompFrame =
        StompFrame(
            command = "MESSAGE",
            headers =
                mapOf(
                    "subscription" to subscriptionId,
                    "destination" to destination,
                    "content-type" to "application/json",
                ),
            body = body,
        )

    fun error(message: String): StompFrame =
        StompFrame(
            command = "ERROR",
            headers = mapOf("message" to message),
        )
}

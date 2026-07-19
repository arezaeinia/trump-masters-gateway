package com.arnia.gateway.service

import com.arnia.gateway.config.RedisTestContainerConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.junit5.WireMockExtension
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import reactor.core.publisher.Flux
import java.net.URI
import java.time.Duration
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.DESedeKeySpec

/**
 * REAL end-to-end integration test:
 * Client WebSocket → Gateway STOMP handler → Decrypt → Backend API
 *
 * This test verifies:
 * 1. WebSocket connection works
 * 2. STOMP protocol is handled correctly (raw STOMP, no SockJS)
 * 3. Encryption/decryption works
 * 4. Gateway calls backend with correct data
 *
 * We DON'T wait for Redis responses (that would timeout).
 * We just verify the backend API was called correctly.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(RedisTestContainerConfig::class)
@ActiveProfiles("test")
class WebSocketGatewayHandlerIT {
    @LocalServerPort
    private var gatewayPort: Int = 0

    private val wsClient = ReactorNettyWebSocketClient()
    private val mapper = ObjectMapper()
    private val testEncryptionKey = "012345678901234567890123".toByteArray()

    companion object {
        @JvmField
        @RegisterExtension
        val wireMock: WireMockExtension =
            WireMockExtension
                .newInstance()
                .options(
                    WireMockConfiguration
                        .wireMockConfig()
                        .usingFilesUnderDirectory("src/test/resources/wiremock")
                        .dynamicPort(),
                ).build()

        @JvmStatic
        @DynamicPropertySource
        fun configureProperties(registry: DynamicPropertyRegistry) {
            registry.add("backend.base-url") { wireMock.baseUrl() }
        }
    }

    @Test
    fun `WebSocket client sends MOVE_CONFIRM and gateway calls backend correctly`() {
        val gameId = 100
        val moveIndex = 5
        val userId = 42
        val authToken = "Bearer test-token"

        // WireMock mappings are loaded automatically from src/test/resources/wiremock/

        // Create game message
        val gameMessage =
            mapOf(
                "type" to "MOVE_CONFIRM",
                "gameId" to gameId,
                "senderUserId" to userId,
                "moveDto" to
                    mapOf(
                        "indices" to intArrayOf(0, 1, 2),
                        "moveIndex" to moveIndex,
                        "ai" to false,
                    ),
            )

        // Encrypt (like client does)
        val encryptedData = encrypt(mapper.writeValueAsString(gameMessage))

        // Outer envelope
        val outerMessage =
            mapOf(
                "gameId" to gameId,
                "encryptedData" to encryptedData,
            )

        // Create raw STOMP frames (matching Unity client behavior - no SockJS)
        val connectFrame = "CONNECT\naccept-version:1.1\nheart-beat:10000,10000\n\n\u0000"
        val sendFrame =
            "SEND\ndestination:/app/game.sendMessage\n" +
                "content-type:application/json;charset=UTF-8\n\n" +
                "${mapper.writeValueAsString(outerMessage)}\u0000"

        // Connect to gateway WebSocket
        val uri = URI.create("ws://localhost:$gatewayPort/ws")

        try {
            wsClient
                .execute(uri) { session ->
                    session
                        .send(
                            Flux.just(
                                session.textMessage(connectFrame),
                                session.textMessage(sendFrame),
                            ),
                        ).thenMany(
                            // Just consume some frames (we don't care about responses for this test)
                            session
                                .receive()
                                .map(WebSocketMessage::getPayloadAsText)
                                .take(2)
                                .timeout(Duration.ofSeconds(2))
                                .onErrorResume { Flux.empty() }, // Ignore timeout errors
                        ).then()
                }.block(Duration.ofSeconds(5))
        } catch (e: Exception) {
            // Ignore WebSocket errors - we only care about backend calls
        }

        // Wait a bit for async processing
        Thread.sleep(500)

        // VERIFY: Backend was called to fetch encryption key
        wireMock.verify(
            getRequestedFor(urlMatching("/api/games/$gameId")),
        )

        // VERIFY: Backend was called with game command
        // THIS IS THE IMPORTANT PART - Verify the WebSocket message was:
        // 1. Received by gateway ✅
        // 2. Decrypted correctly ✅
        // 3. Routed to backend API ✅
        // 4. With correct endpoint, headers, and body ✅

        wireMock.verify(
            postRequestedFor(urlEqualTo("/api/games/$gameId/commands"))
                .withHeader("X-Gateway-Request", equalTo("true"))
                .withRequestBody(matchingJsonPath("$.type", equalTo("MOVE_CONFIRM")))
                .withRequestBody(matchingJsonPath("$.userId", equalTo("$userId")))
                .withRequestBody(matchingJsonPath("$.moveIndex", equalTo("$moveIndex")))
                .withRequestBody(matchingJsonPath("$.indices[0]", equalTo("0")))
                .withRequestBody(matchingJsonPath("$.indices[1]", equalTo("1")))
                .withRequestBody(matchingJsonPath("$.indices[2]", equalTo("2"))),
        )
    }

    @Test
    fun `WebSocket client sends EXIT and gateway routes correctly`() {
        val gameId = 200
        val userId = 99
        val authToken = "Bearer exit-token"

        val gameMessage =
            mapOf(
                "type" to "FAST_EXIT",
                "gameId" to gameId,
                "senderUserId" to userId,
            )

        sendGameMessageViaWebSocket(gameId, gameMessage, authToken)

        Thread.sleep(500)

        wireMock.verify(
            getRequestedFor(urlMatching("/api/games/$gameId")),
        )

        wireMock.verify(
            postRequestedFor(urlEqualTo("/api/games/$gameId/commands"))
                .withHeader("X-Gateway-Request", equalTo("true"))
                .withRequestBody(matchingJsonPath("$.type", equalTo("FAST_EXIT")))
                .withRequestBody(matchingJsonPath("$.userId", equalTo("$userId"))),
        )
    }

    @Test
    fun `WebSocket client sends GET_HINT with moveIndex`() {
        val gameId = 300
        val moveIndex = 10
        val userId = 77

        val gameMessage =
            mapOf(
                "type" to "GET_HINT",
                "gameId" to gameId,
                "senderUserId" to userId,
                "moveDto" to mapOf("moveIndex" to moveIndex),
            )

        sendGameMessageViaWebSocket(gameId, gameMessage, "Bearer hint-token")

        Thread.sleep(500)

        wireMock.verify(
            postRequestedFor(urlEqualTo("/api/games/$gameId/commands"))
                .withRequestBody(matchingJsonPath("$.type", equalTo("GET_HINT")))
                .withRequestBody(matchingJsonPath("$.moveIndex", equalTo("$moveIndex"))),
        )
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    private fun sendGameMessageViaWebSocket(
        gameId: Int,
        gameMessage: Map<String, Any>,
        authToken: String,
    ) {
        val encryptedData = encrypt(mapper.writeValueAsString(gameMessage))
        val outerMessage =
            mapOf(
                "gameId" to gameId,
                "encryptedData" to encryptedData,
            )

        // Raw STOMP frames (no SockJS wrapping)
        val connectFrame = "CONNECT\naccept-version:1.1\nheart-beat:10000,10000\n\n\u0000"
        val sendFrame =
            "SEND\ndestination:/app/game.sendMessage\n" +
                "content-type:application/json;charset=UTF-8\n\n" +
                "${mapper.writeValueAsString(outerMessage)}\u0000"

        val uri = URI.create("ws://localhost:$gatewayPort/ws")

        try {
            wsClient
                .execute(uri) { session ->
                    session
                        .send(
                            Flux.just(
                                session.textMessage(connectFrame),
                                session.textMessage(sendFrame),
                            ),
                        ).thenMany(
                            session
                                .receive()
                                .take(2)
                                .timeout(Duration.ofSeconds(2))
                                .onErrorResume { Flux.empty() },
                        ).then()
                }.block(Duration.ofSeconds(5))
        } catch (e: Exception) {
            // Ignore - we only verify backend calls
        }
    }

    private fun encrypt(plaintext: String): String {
        val keySpec = DESedeKeySpec(testEncryptionKey)
        val keyFactory = SecretKeyFactory.getInstance("DESede")
        val secretKey = keyFactory.generateSecret(keySpec)

        val cipher = Cipher.getInstance("DESede/ECB/PKCS5Padding")
        cipher.init(Cipher.ENCRYPT_MODE, secretKey)

        val encrypted = cipher.doFinal(plaintext.toByteArray(Charsets.UTF_8))
        return Base64.getEncoder().encodeToString(encrypted)
    }
}

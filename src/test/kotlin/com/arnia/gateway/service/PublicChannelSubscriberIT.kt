package com.arnia.gateway.service

import com.arnia.gateway.config.RedisTestContainerConfig
import com.arnia.gateway.registry.SessionRegistry
import com.arnia.gateway.stomp.StompParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.junit5.WireMockExtension
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.kotlin.whenever
import org.reactivestreams.Publisher
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Integration test for PublicChannelSubscriber.
 *
 * Verifies that a message published to Redis on "/topic/public/{gameId}"
 * is received by the subscriber and pushed to the correct WebSocket session
 * as a properly framed raw STOMP MESSAGE frame (no SockJS wrapping).
 *
 * Equivalent to RedisSubscriberIT in baltazar-server, but:
 * - Uses a mock [org.springframework.web.reactive.socket.WebSocketSession] instead of mock [SimpMessagingTemplate]
 * - Verifies raw STOMP framing of the outgoing message
 */
@SpringBootTest
@Import(RedisTestContainerConfig::class)
@ActiveProfiles("test")
class PublicChannelSubscriberIT {
    @Autowired
    private lateinit var stringRedisTemplate: StringRedisTemplate

    @Autowired
    private lateinit var sessionRegistry: SessionRegistry

    private val mapper = ObjectMapper()

    private lateinit var mockSession: WebSocketSession
    private lateinit var capturedMessages: MutableList<String>

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

    @BeforeEach
    fun setUp() {
        mockSession = Mockito.mock(WebSocketSession::class.java)
        capturedMessages = mutableListOf()

        whenever(mockSession.id).thenReturn("test-session-1")
        whenever(mockSession.isOpen).thenReturn(true)
        whenever(mockSession.textMessage(ArgumentMatchers.any())).thenAnswer { invocation ->
            val text = invocation.getArgument<String>(0)
            capturedMessages.add(text)
            Mockito.mock(WebSocketMessage::class.java)
        }
        // The send() method needs to trigger the subscription that calls textMessage()
        whenever(mockSession.send(ArgumentMatchers.any())).thenAnswer { invocation ->
            val publisher = invocation.getArgument<Publisher<WebSocketMessage>>(0)
            Flux.from(publisher).collectList().block()
            Mono.empty<Void>()
        }
    }

    @Test
    fun `should push valid Redis message to registered WebSocket session as STOMP MESSAGE frame`() {
        val gameId = 42
        val channel = "/topic/public/$gameId"
        val subscriptionId = "sub-0"

        sessionRegistry.register(gameId, mockSession, subscriptionId)

        val gameMessage =
            com.arnia.gateway.model.WebSocketMessage(
                type = "MOVE_CONFIRM",
                gameId = gameId,
                senderUserId = 123,
                sender = "testUser",
            )
        val payload = mapper.writeValueAsString(gameMessage)
        stringRedisTemplate.convertAndSend(channel, payload)

        // Wait for async Redis delivery
        Mockito.verify(mockSession, Mockito.timeout(2000)).send(ArgumentMatchers.any())

        Assertions.assertEquals(1, capturedMessages.size)

        // Verify raw STOMP frame (no SockJS wrapping)
        val wireFrame = capturedMessages.first()

        // Parse STOMP frame directly (no unwrapping needed)
        val stompFrame = StompParser.parse(wireFrame)
        Assertions.assertEquals("MESSAGE", stompFrame.command)
        Assertions.assertEquals(subscriptionId, stompFrame.header("subscription"))
        Assertions.assertEquals(channel, stompFrame.header("destination"))
        // Body should be an OuterMessage with encrypted data
        Assertions.assertTrue(stompFrame.body.contains("\"gameId\":$gameId"))
        Assertions.assertTrue(stompFrame.body.contains("\"encryptedData\":"))
    }

    @Test
    fun `should not push when no session is registered for gameId`() {
        val channel = "/topic/public/999"
        val gameMessage =
            com.arnia.gateway.model.WebSocketMessage(
                type = "TEST",
                gameId = 999,
            )
        val payload = mapper.writeValueAsString(gameMessage)

        stringRedisTemplate.convertAndSend(channel, payload)

        // No session registered for gameId 999 — nothing should be sent
        Mockito.verify(mockSession, Mockito.after(500).never()).send(ArgumentMatchers.any())
        Assertions.assertTrue(capturedMessages.isEmpty())
    }

    @Test
    fun `should deregister closed session and not push to it`() {
        val gameId = 77
        val channel = "/topic/public/$gameId"

        whenever(mockSession.isOpen).thenReturn(false)
        sessionRegistry.register(gameId, mockSession, "sub-1")

        val gameMessage =
            com.arnia.gateway.model.WebSocketMessage(
                type = "TEST",
                gameId = gameId,
            )
        val payload = mapper.writeValueAsString(gameMessage)
        stringRedisTemplate.convertAndSend(channel, payload)

        Mockito.verify(mockSession, Mockito.after(1000).never()).send(ArgumentMatchers.any())
        // Session should have been deregistered
        Thread.sleep(200) // Give time for deregistration
        Assertions.assertTrue(sessionRegistry.getEntries(gameId).isEmpty())
    }

    @Test
    fun `should push to all sessions registered for same gameId`() {
        val gameId = 55
        val channel = "/topic/public/$gameId"

        val mockSession2 = Mockito.mock(WebSocketSession::class.java)
        val capturedMessages2 = mutableListOf<String>()

        whenever(mockSession2.id).thenReturn("test-session-2")
        whenever(mockSession2.isOpen).thenReturn(true)
        whenever(mockSession2.textMessage(ArgumentMatchers.any())).thenAnswer { invocation ->
            capturedMessages2.add(invocation.getArgument(0))
            Mockito.mock(WebSocketMessage::class.java)
        }
        whenever(mockSession2.send(ArgumentMatchers.any())).thenAnswer { invocation ->
            val publisher = invocation.getArgument<Publisher<WebSocketMessage>>(0)
            Flux.from(publisher).collectList().block()
            Mono.empty<Void>()
        }

        sessionRegistry.register(gameId, mockSession, "sub-0")
        sessionRegistry.register(gameId, mockSession2, "sub-0")

        val gameMessage =
            com.arnia.gateway.model.WebSocketMessage(
                type = "BROADCAST",
                gameId = gameId,
            )
        val payload = mapper.writeValueAsString(gameMessage)
        stringRedisTemplate.convertAndSend(channel, payload)

        Mockito.verify(mockSession, Mockito.timeout(2000)).send(ArgumentMatchers.any())
        Mockito.verify(mockSession2, Mockito.timeout(2000)).send(ArgumentMatchers.any())

        Assertions.assertEquals(1, capturedMessages.size)
        Assertions.assertEquals(1, capturedMessages2.size)
    }
}

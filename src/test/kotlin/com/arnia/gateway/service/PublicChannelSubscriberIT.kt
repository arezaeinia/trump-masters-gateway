package com.arnia.gateway.service

import com.arnia.gateway.config.RedisTestContainerConfig
import com.arnia.gateway.registry.SessionRegistry
import com.arnia.gateway.stomp.StompParser
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.kotlin.whenever
import org.reactivestreams.Publisher
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Verifies the fan-out: an `EVENT` published by the backend to Redis `/topic/game/{gameId}` is pushed
 * to every registered WebSocket session for that game as a raw STOMP `MESSAGE` frame, with the event
 * body forwarded **as-is** (plain JSON, no encryption, no re-serialisation).
 */
@SpringBootTest
@Import(RedisTestContainerConfig::class)
@ActiveProfiles("test")
class PublicChannelSubscriberIT {
    @Autowired
    private lateinit var stringRedisTemplate: StringRedisTemplate

    @Autowired
    private lateinit var sessionRegistry: SessionRegistry

    private lateinit var mockSession: WebSocketSession
    private lateinit var capturedMessages: MutableList<String>

    @BeforeEach
    fun setUp() {
        mockSession = mockSession("test-session-1", capturedInto = mutableListOf<String>().also { capturedMessages = it })
    }

    private fun mockSession(
        id: String,
        capturedInto: MutableList<String>,
        open: Boolean = true,
    ): WebSocketSession {
        val session = Mockito.mock(WebSocketSession::class.java)
        whenever(session.id).thenReturn(id)
        whenever(session.isOpen).thenReturn(open)
        whenever(session.textMessage(ArgumentMatchers.any())).thenAnswer {
            capturedInto.add(it.getArgument(0))
            Mockito.mock(WebSocketMessage::class.java)
        }
        whenever(session.send(ArgumentMatchers.any())).thenAnswer {
            Flux.from(it.getArgument<Publisher<WebSocketMessage>>(0)).collectList().block()
            Mono.empty<Void>()
        }
        return session
    }

    private fun eventJson(gameId: Int) =
        """{"kind":"EVENT","event":"CARD_PLAYED","gameId":$gameId,"roundNumber":3,"state":{"trickNumber":2,"plays":[]}}"""

    @Test
    fun `pushes a Redis event to the registered session as a STOMP MESSAGE frame, body verbatim`() {
        val gameId = 42
        val channel = "/topic/game/$gameId"
        val subscriptionId = "sub-0"
        sessionRegistry.register(gameId, mockSession, subscriptionId)

        val payload = eventJson(gameId)
        stringRedisTemplate.convertAndSend(channel, payload)

        Mockito.verify(mockSession, Mockito.timeout(2000)).send(ArgumentMatchers.any())
        Assertions.assertEquals(1, capturedMessages.size)

        val frame = StompParser.parse(capturedMessages.first())
        Assertions.assertEquals("MESSAGE", frame.command)
        Assertions.assertEquals(subscriptionId, frame.header("subscription"))
        Assertions.assertEquals(channel, frame.header("destination"))
        // Body forwarded verbatim — no encryptedData wrapper, the raw EVENT JSON.
        Assertions.assertEquals(payload, frame.body)
        Assertions.assertTrue(frame.body.contains("\"event\":\"CARD_PLAYED\""))
        Assertions.assertFalse(frame.body.contains("encryptedData"))
    }

    @Test
    fun `does not push when no session is registered for the gameId`() {
        stringRedisTemplate.convertAndSend("/topic/game/999", eventJson(999))
        Mockito.verify(mockSession, Mockito.after(500).never()).send(ArgumentMatchers.any())
        Assertions.assertTrue(capturedMessages.isEmpty())
    }

    @Test
    fun `deregisters a closed session and does not push to it`() {
        val gameId = 77
        val closed = mockSession("closed-session", capturedInto = mutableListOf(), open = false)
        sessionRegistry.register(gameId, closed, "sub-1")

        stringRedisTemplate.convertAndSend("/topic/game/$gameId", eventJson(gameId))

        Mockito.verify(closed, Mockito.after(1000).never()).send(ArgumentMatchers.any())
        Thread.sleep(200)
        Assertions.assertTrue(sessionRegistry.getEntries(gameId).isEmpty())
    }

    @Test
    fun `pushes to all sessions registered for the same gameId`() {
        val gameId = 55
        val captured2 = mutableListOf<String>()
        val session2 = mockSession("test-session-2", capturedInto = captured2)

        sessionRegistry.register(gameId, mockSession, "sub-0")
        sessionRegistry.register(gameId, session2, "sub-0")

        stringRedisTemplate.convertAndSend("/topic/game/$gameId", eventJson(gameId))

        Mockito.verify(mockSession, Mockito.timeout(2000)).send(ArgumentMatchers.any())
        Mockito.verify(session2, Mockito.timeout(2000)).send(ArgumentMatchers.any())
        Assertions.assertEquals(1, capturedMessages.size)
        Assertions.assertEquals(1, captured2.size)
    }
}

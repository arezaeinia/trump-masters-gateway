package com.arnia.gateway.config

import com.arnia.gateway.service.WebSocketGatewayHandler
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter

@Configuration
class WebSocketConfig(
    private val handler: WebSocketGatewayHandler,
    @Value("\${websocket.path}") private val wsPath: String,
) {
    @Bean
    fun webSocketHandlerMapping(): HandlerMapping {
        val mapping = SimpleUrlHandlerMapping()
        // SockJS transport URLs look like /ws/{serverId}/{sessionId}/websocket
        // so we must match the entire /ws/** subtree, not just /ws
        mapping.urlMap = mapOf("$wsPath/**" to handler)
        mapping.order = 1
        return mapping
    }

    @Bean
    fun handlerAdapter() = WebSocketHandlerAdapter()
}

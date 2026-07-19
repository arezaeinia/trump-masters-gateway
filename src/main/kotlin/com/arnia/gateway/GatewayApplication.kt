package com.arnia.gateway

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class GatewayApplication {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            runApplication<GatewayApplication>(*args)
        }
    }
}

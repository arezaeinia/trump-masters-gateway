package com.arnia.gateway.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.info.Info
import org.springframework.boot.actuate.info.InfoContributor
import org.springframework.stereotype.Component
import java.lang.management.ManagementFactory

@Component
class RuntimeInfoContributor : InfoContributor {
    @Value("\${DEPLOYMENT_HOST:unknown}")
    private val host: String? = null

    /**
     * Contributes runtime information to the Spring Boot /actuator/info endpoint.
     */
    override fun contribute(builder: Info.Builder) {
        builder.withDetail(
            "runtime",
            mapOf(
                "host" to host,
                "uptimeSeconds" to (ManagementFactory.getRuntimeMXBean().uptime / 1000),
            ),
        )
    }
}

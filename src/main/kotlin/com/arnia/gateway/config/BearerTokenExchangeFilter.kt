package com.arnia.gateway.config

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFilterFunction
import org.springframework.web.reactive.function.client.ExchangeFunction
import reactor.core.publisher.Mono

/**
 * WebClient filter that automatically injects Bearer tokens for backend API calls.
 * - Fetches token from TokenService
 * - Adds "Authorization: Bearer <token>" header
 * - Retries once on 401 Unauthorized (with refreshed token)
 * Only active when service.token.enabled=true.
 */
@Component
@ConditionalOnProperty(
    prefix = "service.token",
    name = ["enabled"],
    havingValue = "true",
    matchIfMissing = false,
)
class BearerTokenExchangeFilter(
    private val tokenService: TokenService,
) : ExchangeFilterFunction {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun filter(
        request: ClientRequest,
        next: ExchangeFunction,
    ): Mono<ClientResponse> =
        tokenService
            .getAccessToken()
            .flatMap { token ->
                val authorizedRequest =
                    ClientRequest
                        .from(request)
                        .header("Authorization", "Bearer $token")
                        .build()

                next
                    .exchange(authorizedRequest)
                    .flatMap { response ->
                        if (response.statusCode() == HttpStatus.UNAUTHORIZED) {
                            log.warn("Received 401 Unauthorized, refreshing token and retrying request")
                            response.releaseBody()
                            retryWithRefreshedToken(request, next)
                        } else {
                            Mono.just(response)
                        }
                    }
            }.doOnError { error ->
                log.error("Error in BearerTokenExchangeFilter: {}", error.message)
            }

    private fun retryWithRefreshedToken(
        request: ClientRequest,
        next: ExchangeFunction,
    ): Mono<ClientResponse> =
        tokenService
            .refreshToken()
            .flatMap { newToken ->
                val retryRequest =
                    ClientRequest
                        .from(request)
                        .header("Authorization", "Bearer $newToken")
                        .build()

                next.exchange(retryRequest)
            }
}

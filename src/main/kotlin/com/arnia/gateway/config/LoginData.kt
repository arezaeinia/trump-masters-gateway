package com.arnia.gateway.config

import com.fasterxml.jackson.annotation.JsonProperty

data class LoginData(
    @JsonProperty("access_token")
    val accessToken: String = "",
    @JsonProperty("expires_in")
    val expiresIn: Long = 0,
    @JsonProperty("refresh_token")
    val refreshToken: String = "",
)

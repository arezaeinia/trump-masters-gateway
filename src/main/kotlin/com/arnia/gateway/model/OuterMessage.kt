package com.arnia.gateway.model

data class OuterMessage(
    val gameId: Int,
    val encryptedData: String,
)

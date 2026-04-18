package io.mitochondria.events.order;

public record OrderPlacedEvent(String orderId, String email, String productName, Integer quantity) {}
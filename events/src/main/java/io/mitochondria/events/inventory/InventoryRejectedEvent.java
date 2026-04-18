package io.mitochondria.events.inventory;

public record InventoryRejectedEvent(String orderID, String email) {}
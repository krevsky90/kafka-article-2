package io.mitochondria.events.inventory;

public record InventoryReservedEvent(String orderID, String email) {}
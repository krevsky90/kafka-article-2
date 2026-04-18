package io.mitochondria.inventory.service;

import io.mitochondria.events.order.OrderPlacedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Service
public class InventoryService {
   private final InventoryProcessor inventoryProcessor;

    public InventoryService(InventoryProcessor inventoryProcessor) {
        this.inventoryProcessor = inventoryProcessor;
    }

    @KafkaListener(topics = "order-placed")
    public void reserveInventory(OrderPlacedEvent orderPlacedEvent, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) throws UnknownHostException {
        System.out.println(
                "Instance: " + InetAddress.getLocalHost().getHostName() +
                        " | Partition: " + partition +
                        " | Order: " + orderPlacedEvent.orderId()
        );

        inventoryProcessor.process(orderPlacedEvent);
    }
}
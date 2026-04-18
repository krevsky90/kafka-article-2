package io.mitochondria.krev.service;

import io.mitochondria.events.inventory.InventoryRejectedEvent;
import io.mitochondria.events.inventory.InventoryReservedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KrevService {
    private static final Logger logger = LoggerFactory.getLogger(KrevService.class);

    @KafkaListener(topics = "inventory-reserved")
    public void handle(InventoryReservedEvent inventoryReservedEvent) {
        logger.info("KREV SERVICE Received inventory reserved event: {}", inventoryReservedEvent);
    }

    @KafkaListener(topics = "inventory-rejected")
    public void handle(InventoryRejectedEvent inventoryRejectedEvent) {
        logger.info("KREV SERVICE Received inventory rejected event: {}", inventoryRejectedEvent);
    }

}

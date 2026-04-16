package io.mitochondria.inventory.service;

import io.mitochondria.inventory.event.InventoryRejectedEvent;
import io.mitochondria.inventory.event.InventoryReservedEvent;
import io.mitochondria.inventory.repository.InventoryRepository;
import io.mitochondria.order.event.OrderPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

@Service
public class InventoryService {
    private static final Logger logger = LoggerFactory.getLogger(InventoryService.class);
    private final InventoryRepository inventoryRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryService(InventoryRepository inventoryRepository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.inventoryRepository = inventoryRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // NOTE: @Transactional is MANDATORY since reserveInventory calls inventoryRepository.deductStock,
    // that uses JPA that needs existing transaction!
    @Transactional
    @KafkaListener(topics = "order-placed")
    public void reserveInventory(OrderPlacedEvent orderPlacedEvent, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) throws UnknownHostException {
        System.out.println(
                "Instance: " + InetAddress.getLocalHost().getHostName() +
                        " | Partition: " + partition +
                        " | Order: " + orderPlacedEvent.orderId()
        );

        int count = inventoryRepository.deductStock(orderPlacedEvent.productName(), orderPlacedEvent.quantity());
        if (count > 0) {
            InventoryReservedEvent inventoryReservedEvent = new InventoryReservedEvent(
                    orderPlacedEvent.orderId(),
                    orderPlacedEvent.email()
            );

            kafkaTemplate.send("inventory-reserved", inventoryReservedEvent.orderID(), inventoryReservedEvent);
        } else {

            InventoryRejectedEvent inventoryRejectedEvent = new InventoryRejectedEvent(
                    orderPlacedEvent.orderId(),
                    orderPlacedEvent.email()
            );

            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send("inventory-rejected", inventoryRejectedEvent.orderID(), inventoryRejectedEvent);

            future.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    logger.info("Error sending inventory rejected event in thread {}", Thread.currentThread().getName());
                } else {
                    logger.info("Success sending inventory rejected event in thread {}", Thread.currentThread().getName());
                }
            });
        }
    }
}
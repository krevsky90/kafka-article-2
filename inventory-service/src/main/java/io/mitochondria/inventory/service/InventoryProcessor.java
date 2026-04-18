package io.mitochondria.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mitochondria.events.order.OrderPlacedEvent;
import io.mitochondria.inventory.dto.InventoryRejectedDto;
import io.mitochondria.inventory.dto.InventoryReservedDto;
import io.mitochondria.inventory.exception.NonRetryableException;
import io.mitochondria.inventory.exception.RetryableException;
import io.mitochondria.inventory.model.OutboxEvent;
import io.mitochondria.inventory.model.ProcessedOrderId;
import io.mitochondria.inventory.repository.InventoryRepository;
import io.mitochondria.inventory.repository.OutboxEventRepository;
import io.mitochondria.inventory.repository.ProcessedOrderIdRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryProcessor {
    private static final Logger logger = LoggerFactory.getLogger(InventoryProcessor.class);
    private final InventoryRepository inventoryRepository;
    private final ProcessedOrderIdRepository processedOrderIdRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public InventoryProcessor(InventoryRepository inventoryRepository, ProcessedOrderIdRepository processedOrderIdRepository, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.inventoryRepository = inventoryRepository;
        this.processedOrderIdRepository = processedOrderIdRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    // NOTE: @Transactional is MANDATORY since reserveInventory calls inventoryRepository.deductStock,
    // that uses JPA that needs existing transaction!
    @Transactional
    public void process(OrderPlacedEvent orderPlacedEvent) {
        String orderId = orderPlacedEvent.getOrderId().toString();
        String productName = orderPlacedEvent.getProductName().toString();
        String email = orderPlacedEvent.getEmail().toString();
        int quantity = orderPlacedEvent.getQuantity();

        //deduplication check
        try {
            processedOrderIdRepository.save(new ProcessedOrderId(orderId));
        } catch (DataIntegrityViolationException ex) {
            logger.info("Order {} already processed", orderId);
            return;
        }

        int count;
        try {
            count = inventoryRepository.deductStock(productName, quantity);
        } catch (Exception ex) {
            throw new RetryableException("Database error", ex);
        }
        String topic = count > 0 ? "inventory-reserved" : "inventory-rejected";
        //this is json dto! not avro object!
        Object dto = count > 0 ?
                new InventoryReservedDto(
                        orderId,
                        email
                ) :
                new InventoryRejectedDto(
                        orderId,
                        email
                );

        String eventAsString;
        try {
            eventAsString = objectMapper.writeValueAsString(dto);
        } catch (Exception e) {
            throw new NonRetryableException("Serialization failed for order: " + orderId, e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(
                orderId,
                topic,
                eventAsString
        );

        try {
            outboxEventRepository.save(outboxEvent);
        } catch (Exception e) {
            throw new RetryableException("Failed to save outbox event", e);
        }
    }
}

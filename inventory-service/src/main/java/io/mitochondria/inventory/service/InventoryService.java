package io.mitochondria.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mitochondria.inventory.event.InventoryRejectedEvent;
import io.mitochondria.inventory.event.InventoryReservedEvent;
import io.mitochondria.inventory.model.OutboxEvent;
import io.mitochondria.inventory.model.ProcessedOrderId;
import io.mitochondria.inventory.repository.InventoryRepository;
import io.mitochondria.inventory.repository.OutboxEventRepository;
import io.mitochondria.inventory.repository.ProcessedOrderIdRepository;
import io.mitochondria.order.event.OrderPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Service
public class InventoryService {
    private static final Logger logger = LoggerFactory.getLogger(InventoryService.class);
    private final InventoryRepository inventoryRepository;
    private final ProcessedOrderIdRepository processedOrderIdRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public InventoryService(InventoryRepository inventoryRepository, ProcessedOrderIdRepository processedOrderIdRepository, OutboxEventRepository outboxEventRepository, KafkaTemplate<String, Object> kafkaTemplate, ObjectMapper objectMapper) {
        this.inventoryRepository = inventoryRepository;
        this.processedOrderIdRepository = processedOrderIdRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
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

        //deduplication check
        try {
            processedOrderIdRepository.save(new ProcessedOrderId(orderPlacedEvent.orderId()));
        } catch (DataIntegrityViolationException ex) {
            logger.info("Order {} already processed", orderPlacedEvent.orderId());
            return;
        }

        int count = inventoryRepository.deductStock(orderPlacedEvent.productName(), orderPlacedEvent.quantity());
        String topic = count > 0 ? "inventory-reserved" : "inventory-rejected";
        Object event = count > 0 ?
                new InventoryReservedEvent(
                        orderPlacedEvent.orderId(),
                        orderPlacedEvent.email()
                ) :
                new InventoryRejectedEvent(
                        orderPlacedEvent.orderId(),
                        orderPlacedEvent.email()
                );

        String eventAsString;
        try {
            eventAsString = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed for order: " + orderPlacedEvent.orderId(), e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(
                orderPlacedEvent.orderId(),
                topic,
                eventAsString
        );

        outboxEventRepository.save(outboxEvent);
    }
}
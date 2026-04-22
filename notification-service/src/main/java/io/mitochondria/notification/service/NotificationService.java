package io.mitochondria.notification.service;

import io.mitochondria.events.inventory.InventoryRejectedEvent;
import io.mitochondria.events.inventory.InventoryReservedEvent;
import io.mitochondria.notification.domain.UserInfo;
import io.mitochondria.notification.model.ProcessedOrderId;
import io.mitochondria.notification.port.UserInfoClient;
import io.mitochondria.notification.repository.ProcessedOrderIdRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {
    private static final Logger logger = LoggerFactory.getLogger(NotificationService.class);
    private final ProcessedOrderIdRepository processedOrderIdRepository;
    private final UserInfoClient userInfoClient;

    public NotificationService(ProcessedOrderIdRepository processedOrderIdRepository, UserInfoClient userInfoClient) {
        this.processedOrderIdRepository = processedOrderIdRepository;
        this.userInfoClient = userInfoClient;
    }

    @KafkaListener(topics = "inventory-reserved")
    public void sendNotificationIfReserved(InventoryReservedEvent inventoryReservedEvent) {
        //deduplication check
        try {
            processedOrderIdRepository.save(new ProcessedOrderId(inventoryReservedEvent.getOrderId().toString()));
        } catch (DataIntegrityViolationException ex) {
            logger.info("Order {} already processed", inventoryReservedEvent.getOrderId().toString());
            return;
        }

        String email = inventoryReservedEvent.getEmail().toString();
        UserInfo userInfo = userInfoClient.getUserInfo(email);

        logger.info("Received inventory reserved event: {}", inventoryReservedEvent);
        logger.info("userInfo: {}", userInfo);
    }

    @KafkaListener(topics = "inventory-rejected")
    public void sendNotificationIfRejected(InventoryRejectedEvent inventoryRejectedEvent) {
        //deduplication check
        try {
            processedOrderIdRepository.save(new ProcessedOrderId(inventoryRejectedEvent.getOrderId().toString()));
        } catch (DataIntegrityViolationException ex) {
            logger.info("Order {} already processed", inventoryRejectedEvent.getOrderId().toString());
            return;
        }

        String email = inventoryRejectedEvent.getEmail().toString();
        UserInfo userInfo = userInfoClient.getUserInfo(email);

        logger.info("Received inventory rejected event: {}", inventoryRejectedEvent);
        logger.info("userInfo: {}", userInfo);
    }
}
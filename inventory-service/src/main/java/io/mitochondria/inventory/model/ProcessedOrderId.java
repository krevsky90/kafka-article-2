package io.mitochondria.inventory.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_order_id")
public class ProcessedOrderId {
    @Id
    @Column(name = "order_id", length = 36)
    private String orderId;

    @Column(name = "processed_at")
    private LocalDateTime processedAt;

    public ProcessedOrderId(String orderId) {
        this.orderId = orderId;
        this.processedAt = LocalDateTime.now();
    }

    public ProcessedOrderId() {}

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }
}

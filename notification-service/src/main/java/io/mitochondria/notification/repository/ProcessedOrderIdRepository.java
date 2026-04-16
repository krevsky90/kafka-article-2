package io.mitochondria.notification.repository;

import io.mitochondria.notification.model.ProcessedOrderId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedOrderIdRepository extends JpaRepository<ProcessedOrderId, String> {
}

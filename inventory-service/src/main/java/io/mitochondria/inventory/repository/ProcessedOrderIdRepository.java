package io.mitochondria.inventory.repository;

import io.mitochondria.inventory.model.ProcessedOrderId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedOrderIdRepository extends JpaRepository<ProcessedOrderId, String> {
}

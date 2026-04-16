package io.mitochondria.inventory.model;

import jakarta.persistence.*;

@Entity
@Table(name = "inventory")
public class Inventory {
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "product_name", nullable = false, unique = true, length = 256)
    private String productName;

    @Column(name = "quantity", nullable = false)
    private Integer quantity;

    public Inventory(String productName, Integer quantity) {
        this.productName = productName;
        this.quantity = quantity;
    }

    public Inventory() {}

    public Long getId() {
        return id;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }
}

package com.poc.model;

/**
 * Event model for the HTTP/API source (sales-api).
 * Has a different schema from SaleEvent — mapped before the union.
 */
public class OrderEvent {

    public String orderId;
    public String sellerId;
    public String sellerName;
    public String location;
    public String productId;
    public int    quantity;
    public double totalPrice;
    public String orderDate;
    public String channel;     // "online", "in-store", "phone"
    public String source;

    public OrderEvent() {}

    @Override
    public String toString() {
        return String.format("OrderEvent{id=%s, seller=%s, location=%s, totalPrice=%.2f, channel=%s}",
            orderId, sellerName, location, totalPrice, channel);
    }
}

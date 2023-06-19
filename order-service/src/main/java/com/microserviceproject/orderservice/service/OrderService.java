package com.microserviceproject.orderservice.service;

import com.microserviceproject.orderservice.dto.InventoryResponse;
import com.microserviceproject.orderservice.dto.OrderLineItemsDto;
import com.microserviceproject.orderservice.dto.OrderRequest;
import com.microserviceproject.orderservice.event.OrderPlacedEvent;
import com.microserviceproject.orderservice.model.Order;
import com.microserviceproject.orderservice.model.OrderLineItems;
import com.microserviceproject.orderservice.repository.OrderRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderService {

    private final OrderRepository orderRepository;
    private final WebClient.Builder webClientBuilder;
    private final Tracer tracer;
    private final KafkaTemplate<String,OrderPlacedEvent> kafkaTemplate;

    public String placeOrder(OrderRequest orderRequest){
        Order order = new Order();
        order.setOrderNumber(UUID.randomUUID().toString());

        List<OrderLineItems> orderLineItemsList = orderRequest.getOrderLineItemsDtoList()
                .stream()
                .map(this::mapToDto)
                .toList();

        order.setOrderLineItemsList(orderLineItemsList);

        List<String> skuCodes =order.getOrderLineItemsList().stream()
                .map(OrderLineItems::getSkuCode)
                .toList();

        Span inventoryServiceLookup = tracer.nextSpan().name("InventoryServiceLookup");

        try (Tracer.SpanInScope spanInScope = tracer.withSpan(inventoryServiceLookup.start())){
                InventoryResponse[] inventoryResponseArray = webClientBuilder.build().get()
                        .uri("http://inventory-service/api/inventory", uriBuilder -> uriBuilder.queryParam("skuCode",skuCodes).build())
                        .retrieve()
                        .bodyToMono(InventoryResponse[].class)
                        .block();
                boolean allProductsInStock = Arrays.stream(inventoryResponseArray)
                        .allMatch(InventoryResponse::isInStock);

                if(allProductsInStock){
                    orderRepository.save(order);
                    kafkaTemplate.send("notificationTopic", new OrderPlacedEvent(order.getOrderNumber()));
                    return "Order Placed Successfully";
                }
                else{
                    throw new IllegalArgumentException("Product not in stock please try later");
                }
        } finally {
            inventoryServiceLookup.end();
        }

    }

    private OrderLineItems mapToDto(OrderLineItemsDto orderLineItemsDto) {
        OrderLineItems orderLineItem = new OrderLineItems();
        orderLineItem.setQuantity(orderLineItemsDto.getQuantity());
        orderLineItem.setPrice(orderLineItemsDto.getPrice());
        orderLineItem.setSkuCode(orderLineItemsDto.getSkuCode());
        return orderLineItem;

    }
}

package com.microserviceproject.orderservice.service;

import com.microserviceproject.orderservice.dto.OrderLineItemsDto;
import com.microserviceproject.orderservice.dto.OrderRequest;
import com.microserviceproject.orderservice.model.Order;
import com.microserviceproject.orderservice.model.OrderLineItems;
import com.microserviceproject.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;

    public void placeOrder(OrderRequest orderRequest){
        Order order = new Order();
        order.setOrderNumber(UUID.randomUUID().toString());

        List<OrderLineItems> orderLineItemsList = orderRequest.getOrderLineItemsDtoList()
                .stream()
                .map(this::mapToDto)
                .toList();

        order.setOrderLineItemsList(orderLineItemsList);
        orderRepository.save(order);
    }

    private OrderLineItems mapToDto(OrderLineItemsDto orderLineItemsDto) {
        OrderLineItems orderLineItem = new OrderLineItems();
        orderLineItem.setQuantity(orderLineItemsDto.getQuantity());
        orderLineItem.setPrice(orderLineItemsDto.getPrice());
        orderLineItem.setSkuCode(orderLineItemsDto.getSkuCode());
        return orderLineItem;

    }
}

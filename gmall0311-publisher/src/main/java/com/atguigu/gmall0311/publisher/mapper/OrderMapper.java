package com.atguigu.gmall0311.publisher.mapper;

import com.atguigu.gmall0311.publisher.bean.OrderHourAmount;

public interface OrderMapper {
    public Double getOrderAmount(String date);

    public OrderHourAmount getOrderHourAmout(String date);
}

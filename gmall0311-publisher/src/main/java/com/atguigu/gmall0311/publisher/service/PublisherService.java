package com.atguigu.gmall0311.publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;


public interface PublisherService {

    public Long getDauTotal(String date);

    public Map<String,Long> getDauHoutCount(String date);

    public Double getOrderAmount(String date);
    public Map<String,Double> getOrderHourAmount(String date);
}

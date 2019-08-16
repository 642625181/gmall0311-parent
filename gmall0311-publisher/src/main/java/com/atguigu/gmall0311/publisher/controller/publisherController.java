package com.atguigu.gmall0311.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0311.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.*;

@RestController
public class publisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {
        Long dauTotal = publisherService.getDauTotal(date);
        List<Map> totalList = new ArrayList<>();
        Map dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);
        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id", "dau");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 33333);
        totalList.add(newMidMap);

        String string = JSON.toJSONString(totalList);

        return string;
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        if ("dau".equals((id))) {
            Map<String, Long> dauHourCountTodayMap = publisherService.getDauHoutCount(date);
            String ydate = getYesterdayString(date);
            Map<String, Long> dauHourCountYDayMap = publisherService.getDauHoutCount(ydate);

            Map dauMap = new HashMap();

            dauMap.put("today",dauHourCountTodayMap);
            dauMap.put("yesterday",dauHourCountYDayMap);

            return JSON.toJSONString(dauMap);
        }else {

        }

        return null;
    }

     private String getYesterdayString(String todayString){
         SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
         String yesterdayString = null;
         try {
             Date today = simpleDateFormat.parse(todayString);
             Date yesterday = DateUtils.addDays(today, -1);
             yesterdayString = simpleDateFormat.format(yesterday);
         } catch (ParseException e) {
             e.printStackTrace();
         }
         return yesterdayString;
     }

}


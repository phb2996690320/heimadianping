package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @Description
 * @Author PHB
 * @Date 2024/9/3
 */
@Component
public class RedisIdWorker {
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    private static final int COUNT_BTS = 32;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    public long nextId(String keyPrefix){
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long epochSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = epochSecond - BEGIN_TIMESTAMP;
        //2.生成序列号
        String yyyyMMdd = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long increment = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + yyyyMMdd);
        //3.拼接并且返回

        return timestamp << COUNT_BTS |increment;
    }
}

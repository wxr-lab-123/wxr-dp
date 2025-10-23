package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.sql.Time;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
public class CacheClient {
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    private StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value , Long time , TimeUnit unit) {

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }
    public void setWithLogicalExpire(String key, Object value, Long time,TimeUnit unit) {
        // 设置 逻辑过期0
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds( time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
    /**
     * 使用缓存空值方式解决缓存穿透问题
     *
     * @param id 店铺ID
     * @return Shop 店铺对象，如果不存在则返回null
     */
    public <T,ID> T queryWithPassThrough(String keyPrefix, ID id, Class<T> type, Function<ID,T> dbFallback,Long time , TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.在redis中查询缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
        // 2.缓存命中.返回
        if (StrUtil.isNotBlank(Json)) {
            return  JSONUtil.toBean(Json,type);
        }
        if (Json != null) {
            return null;
        }
        // 3.缓存未命中
        // 4.查询数据库
        T t = dbFallback.apply(id);
        if (t == null) {
            // 缓存空值
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 5.数据库不存在,返回错误
            return null;
        }
        // 6.数据库存在,写入缓存,并设置超时时间
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(t), time, unit);
        // 7.返回
        return t;

    }

    /**
     * 使用逻辑过期方式解决缓存击穿问题
     *
     * @param id 店铺ID
     * @return Shop 店铺对象，如果不存在则返回null
     */
    public <R,ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.在redis 中查询缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
        // 2.缓存未命中.返回
        if (StrUtil.isBlank(Json)) {
            return null;
        }
        // 3.缓存命中
        // json -> 对象
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        JSONObject jsonData = (JSONObject) redisData.getData();
        R r = jsonData.toBean(type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 缓存未过期.直接返回
            return r;
        }
        // 缓存已过期.获取锁
        String lock = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lock);

        if (isLock) {
            // 获取成功,创建新的独立线程,重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 创建重建缓存
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key,r1 , time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unLock(lock);
                }
            });
        }
        // 获取锁失败.返回数据
        // 7.返回
        return r;

    }

    /**
     * 尝试获取分布式锁
     *
     * @param key 锁的键名
     * @return boolean 是否获取成功
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10L, TimeUnit.SECONDS);
        return BooleanUtil.isTrue( flag);
    }

    /**
     * 释放分布式锁
     *
     * @param key 锁的键名
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

}

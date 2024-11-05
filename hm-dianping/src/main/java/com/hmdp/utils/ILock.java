package com.hmdp.utils;

/**
 * @Description
 * @Author PHB
 * @Date 2024/11/2
 */
public interface ILock {

    boolean tryLock(long timeoutSec);

    void unlock();
}

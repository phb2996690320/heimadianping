package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author PHB
 * @Date 2024/8/26
 */

public class LoginInterceptor implements HandlerInterceptor {

    StringRedisTemplate stringRedisTemplate;

    public LoginInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //获取请求头中的token
//        String token = request.getHeader("authorization");
//        if (token == null){
//            return false;
//        }
////        HttpSession session = request.getSession();
////        Object user = session.getAttribute("user");
//        String key =RedisConstants.LOGIN_USER_KEY + token;
//        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(RedisConstants.LOGIN_USER_KEY + token);
//        if (entries.isEmpty()){
//            response.setStatus(401);
//            return false;
//        }
//
//        UserDTO userDTO = BeanUtil.fillBeanWithMap(entries, new UserDTO(), false);
//        UserHolder.saveUser(userDTO);
//        stringRedisTemplate.expire(key,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (UserHolder.getUser() == null) {
            response.setStatus(401);
            return false;
        }
        return true;
    }


    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {

    }
}

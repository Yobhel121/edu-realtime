package com.connection;

import redis.clients.jedis.Jedis;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-11-02 17:29
 **/
public class RedisConnectionTest {

    public static void main(String[] args) {
        // Replace these values with your Redis server configuration
        String host = "8xj0202144.zicp.fun";
        int port = 24046; // Default Redis port

        Jedis jedis = new Jedis(host, port);

        try {
            // Test the connection
            String pong = jedis.ping();
            System.out.println("Connected to Redis: " + pong);
        } catch (Exception e) {
            System.err.println("Failed to connect to Redis");
            e.printStackTrace();
        } finally {
            jedis.close();
        }
    }
}

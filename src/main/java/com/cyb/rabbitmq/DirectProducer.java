package com.cyb.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * @author cyb
 * @date 2024/12/25 18:56
 **/
public class DirectProducer {
    /**
     * 交换机名称
     */
    private final static String EXCHANGE_NAME = "direct_exchange_logs";

    public static void main(String[] argv) throws Exception {
        // 创建工厂
        ConnectionFactory factory = new ConnectionFactory();
        // 设置rabbitmq服务的相关参数
        factory.setHost("localhost");
        // 使用Java7的语法 try-resource，自动关闭资源

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 声明交换机
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");
            // 控制台输入，支持发送多条消息
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String input = scanner.nextLine();
                String[] split = input.split(",");
                String message = split[0];
                String routingKey = split[1];
                // 发送消息
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + message + "'" );
            }
        }
    }
}


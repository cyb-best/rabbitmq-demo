package com.cyb.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

/**
 * @author cyb
 * @date 2024/12/25 18:57
 **/
public class DirectConsumer {

    /**
     * 交换机名称
     */
    private final static String EXCHANGE_NAME = "direct_exchange_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        String queue_name1 = "order_queue";
        channel.queueDeclare(queue_name1, true, false, false, null);
        // 注意 routeingKey不能直接设置为null，会报错，说不能为空
        channel.queueBind(queue_name1, EXCHANGE_NAME, "order");
        Channel channel2 = connection.createChannel();
        channel2.exchangeDeclare(EXCHANGE_NAME, "direct");
        String queue_name2 = "user_queue";
        channel2.queueDeclare(queue_name2, true, false, false, null);
        channel2.queueBind(queue_name2, EXCHANGE_NAME, "user");

        System.out.println(" consumers Waiting for messages. To exit press CTRL+C");

        // 设置回调
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [订单系统] Received '" + message + "'");
        };
        // 两个消费者
        DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [用户系统] Received '" + message + "'");
        };

        // 创建并启动一个消费者 创建消费者1
        channel.basicConsume(queue_name1, true, deliverCallback, consumerTag -> {
        });
        // 创建消费者2
        channel2.basicConsume(queue_name2, true, deliverCallback2, consumerTag -> {
        });
    }
}


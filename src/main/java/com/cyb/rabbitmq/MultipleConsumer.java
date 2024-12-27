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
public class MultipleConsumer {

    private final static String QUEUE_NAME = "work_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 设置每个消费者的预取数量（可以调整为适合的数量）
        // 每次只分配两个个消息给消费者，确保每个消费者逐一处理任务，默认为1
        channel.basicQos(2);
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" consumers Waiting for messages. To exit press CTRL+C");
        // 设置回调
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [consumer1] Received '" + message + "'");
            try {
                System.out.println("Consumer1 正在处理任务中");
                Thread.sleep(6000);
                System.out.println("任务处理完毕 \n");
            } catch (InterruptedException e) {
                System.out.println("Consumer1处理任务失败");
            }
        };
        // 两个消费者
        DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [consumer2] Received '" + message + "'");
            try {
                System.out.println("Consumer2 正在处理任务中");
                Thread.sleep(6000);
                System.out.println("任务处理完毕 \n");
            } catch (InterruptedException e) {
                System.out.println("Consumer2处理任务失败");
            }
        };

        // 创建并启动一个消费者 创建消费者1
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
        // 创建消费者2
        channel.basicConsume(QUEUE_NAME, true, deliverCallback2, consumerTag -> {
        });
    }
}


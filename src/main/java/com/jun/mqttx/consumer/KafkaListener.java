package com.jun.mqttx.consumer;

import com.jun.mqttx.config.ClusterConfig;
import com.jun.mqttx.config.MqttxConfig;
import com.jun.mqttx.constants.InternalMessageEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

/**
 * kafka 消息监听器
 *
 * @author Jun
 * @version 1.0.5
 */
@Slf4j
public class KafkaListener implements Runnable {

    private KafkaConsumer<String, byte[]> kafkaConsumer;
    private InternalMessageSubscriber messageSubscriber;

    public KafkaListener(MqttxConfig mqttxConfig, KafkaProperties kafkaProperties, InternalMessageSubscriber messageSubscriber) {
        Assert.notNull(kafkaProperties, "kafkaProperties can't be null");
        Assert.notNull(messageSubscriber, "messageSubscriber can't be null");

        this.messageSubscriber = messageSubscriber;
        this.kafkaConsumer = new KafkaConsumer<>(kafkaProperties.buildConsumerProperties());

        // kafka 订阅的桥接消息主题
        Set<String> subTopics = mqttxConfig.getMessageQueueBridge().getSubTopics();
        // 基于 kafka 实现集群消息传递
        String type = mqttxConfig.getCluster().getType();
        if (ClusterConfig.KAFKA.equalsIgnoreCase(type)) {
            subTopics.addAll(Arrays.asList(InternalMessageEnum.CLUSTER_CHANNELS));
        }

        this.kafkaConsumer.subscribe(subTopics);
        new Thread(this, this.getClass().getName()).start();
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, byte[]> records = null;
            try {
                records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
                    messageSubscriber.handleBridgeMessage(record.topic(), record.value());
                }
            } catch (Exception e) {
                errorHandle(e, records);
            }
        }
    }

    /**
     * 异常处理方法
     *
     * @param records MQ 中拉取的数据集
     */
    private void errorHandle(Throwable throwable, ConsumerRecords<String, byte[]> records) {
        // do-it-yourself
        log.error("kafka 消息处理异常", throwable);
    }
}

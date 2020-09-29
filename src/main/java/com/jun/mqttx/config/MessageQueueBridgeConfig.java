package com.jun.mqttx.config;

import com.jun.mqttx.consumer.InternalMessageSubscriber;
import com.jun.mqttx.consumer.KafkaListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 消息桥接配置
 *
 * @author Jun
 * @version 1.0.5
 */
@Configuration
@ConditionalOnProperty(name = "mqttx.message-queue-bridge.type", havingValue = "SUB", matchIfMissing = true)
public class MessageQueueBridgeConfig {

    @Bean
    @ConditionalOnExpression("${mqttx.message-queue-bridge.type:'SUB'} eq 'PUB' || ${mqttx.message-queue-bridge.type:'SUB'} eq 'ALL'")
    public KafkaProducer<String, byte[]> kafkaProducer(KafkaProperties kafkaProperties) {
        return new KafkaProducer<>(kafkaProperties.buildProducerProperties());
    }

    @Bean
    @ConditionalOnExpression("${mqttx.message-queue-bridge.type:'SUB'} eq 'SUB' || ${mqttx.message-queue-bridge.type:'SUB'} eq 'ALL'")
    public KafkaListener kafkaListener(MqttxConfig mqttxConfig, KafkaProperties kafkaProperties, InternalMessageSubscriber internalMessageSubscriber) {
        return new KafkaListener(mqttxConfig, kafkaProperties, internalMessageSubscriber);
    }
}

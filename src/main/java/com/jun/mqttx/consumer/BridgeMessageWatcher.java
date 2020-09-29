package com.jun.mqttx.consumer;

/**
 * 桥接消息观察者
 *
 * @author Jun
 * @version 1.0.5
 */
public interface BridgeMessageWatcher {

    void action(String topic, byte[] data);
}

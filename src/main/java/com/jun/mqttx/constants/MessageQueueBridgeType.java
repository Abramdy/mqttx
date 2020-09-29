package com.jun.mqttx.constants;

/**
 * 消息队列桥接类型
 *
 * @author Jun
 * @version 1.0.5
 */
public enum MessageQueueBridgeType {
    //@formatter:off

    /** MQ 仅订阅 */
    SUB,

    /** MQ 仅发布 */
    PUB,

    /** MQ 订阅且发布 */
    ALL
}

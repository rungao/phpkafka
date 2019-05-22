## stubs目录
封装开源的kwn/php-rdkafka-stubs库，加入后kafka相关的类、函数使用会有对应的编辑提示，
支持主流的phostrom等编辑器

## class目录
1、rdkafka封装的PHP类库，支持自定义主题生产，消费支持高阶、低阶的消费，能方面的进行重跑数据支持。
2、支持提供错误的回调方法封装，方面将错误信息写入到业务对应的日志目录中。

[![kafka version support](https://img.shields.io/badge/kafka-0.8%200.9%201.0%201.1%20or%201.1%2B-brightgreen.svg)](#) [![php version support](https://img.shields.io/badge/php-5.3%2B-green.svg)](#) [![librdkafka version support](https://img.shields.io/badge/librdkafka-3.0.5%2B-yellowgreen.svg)](#) [![php-librdkafka](https://img.shields.io/badge/php--librdkafka-3.0.5%2B-orange.svg)](#)

## 扩展安装
> pecl install rdkafka

## 库安装
> composer require octopus/rdkafka

## 使用方法

### 高阶消费者示例

```php
use Octopus\Consumer;
$config = [
    'brokers' => 'localhost:9092',
    'log_level' => LOG_DEBUG
];
$offset = RD_KAFKA_OFFSET_STORED;
$topic = 'ts_click';
$groupId = 'ts_click_group';
$partitionNum = 0;
$consumer = new Octopus\Consumer($config);
$consumer->setConsumerGroup($groupId)
    ->setBrokerServer($config['brokers'])
    ->setTopic($topic, $partitionNum, $offset)
    ->subscribe($topic)
    ->consumer(function($msg){
        var_dump($msg);
    });
```

### 低阶消费者示例

```php

use Octopus\Consumer;
$config = [
    'brokers' => 'localhost:9092',
    'log_level' => LOG_DEBUG
];
$topic = 'ts_click';
$groupId = 'ts_click_group';
// 消费分区(多分区可以启多个进程)
$partitionNum = 0;
// 消费开始点(默认从上次记录的点)
$offset = RD_KAFKA_OFFSET_STORED;
$consumer = new Octopus\Consumer($config);
$consumer->setConsumerGroup($groupId)
    ->setBrokerServer($config['brokers'])
    // 自定义设置分区，消费开始点
    ->setTopic($topic, $partitionNum, $offset)
    // 自定义C端参数设置
    ->setTopicConf('request.required.acks', -1)
    ->subscribe($topic, \Octopus\Consumer::LOW_LEVEL)
    ->consumer(function($msg){
        // 实体业务处理代码
        var_dump($msg);
    });     
```


### 生产者示例
```php
use Octopus\Producer;
$config = [
    'brokers' => 'localhost:9092',
    'log_level' => LOG_DEBUG
];
$producer = new Octopus\Producer($config);
$producer->setBrokerServer()
    ->setProducerTopic('ts_click')
    ->producer($msg);
```

## 初始化类更多配置支持
```php
$defaultConfig = [
    // 生产的dr回调
    'dr_msg_cb' => [$this, 'defaultDrMsg'],
    // 错误回调
    'error_cb' => [$this, 'defaultErrorCb'],
    // 负载回调，你可以用匿名方法自定义
    'rebalance_cb' => [$this, 'defaultRebalance']
];

# broker（消费者）相关配置，参考Configuration.md
$brokerConfig = [
    'request.required.acks'=> -1,
    'auto.commit.interval.ms'=> 100,
    'auto.offset.reset'=> 'smallest',
];
```

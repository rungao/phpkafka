## stubs目录
封装开源的kwn/php-rdkafka-stubs库，加入后kafka相关的类、函数使用会有对应的编辑提示，
支持主流的phostrom等编辑器

## class目录
1、rdkafka封装的PHP类库，支持自定义主题生产，消费支持高阶、低阶的消费，能方面的进行重跑数据支持。
2、支持提供错误的回调方法封装，方面将错误信息写入到业务对应的日志目录中。

[![kafka version support](https://img.shields.io/badge/kafka-0.8%200.9%201.0%201.1%20or%201.1%2B-brightgreen.svg)](#) [![php version support](https://img.shields.io/badge/php-5.3%2B-green.svg)](#) [![librdkafka version support](https://img.shields.io/badge/librdkafka-3.0.5%2B-yellowgreen.svg)](#) [![php-librdkafka](https://img.shields.io/badge/php--librdkafka-3.0.5%2B-orange.svg)](#)

## 目录

1. [安装](#安装)
2. [使用](#使用)
   * [消费](#消费)
   * [生产](#生产)
3. [更多配置](#更多配置)

## 安装
> pecl install rdkafka

## 使用方法

### 高阶消费者示例

```php
use Octopus\Consumer;
// 开始消费点(重复消费使用，如不设置默认从头消费)
$offset = RD_KAFKA_OFFSET_STORED; 
$topic = 'ts_click';
$consumer = new Octopus\Consumer(['ip'=>'127.0.0.1:9002']);
$consumer->setConsumerGroup('test-110-sxx1')
     ->setBrokerServer('127.0.0.1')
     ->setConsumerTopic()
     ->setTopic('ts_click', 0, $offset)
     ->subscribe(['ts_click'])
     ->consumer(function($msg){
         var_dump($msg);
     });
```

### 低阶消费者示例

```php
use Octopus\Consumer;
// 开始消费点(重复消费使用，如不设置默认从头消费)
$offset = RD_KAFKA_OFFSET_STORED; 
$topic = 'ts_click';
$consumer = new Octopus\Consumer(['ip'=>'127.0.0.1:9002']);
$consumer->setConsumerGroup('test-110-sxx1')
     ->setBrokerServer('127.0.0.1')
     ->setConsumerTopic()
     ->setTopic('ts_click', 0, $offset)
     ->subscribe(['ts_click'])
     ->consumer(function($msg){
         var_dump($msg);
     });
```


### 生产者示例
```php
use Octopus\Producer;
$config = [
    // brokder列表，多个使用逗号分隔
    'brokers'=>'127.0.0.1',
    'log_path'=> '/logs/',
    // 自定义错误回调
    'dr_msg_cb' => function($kafka, $message) {
        var_dump((array)$message);
    },
    'error_cb' => function(){},
    'rebalance_cb' => function(){}
];
$producer = new \Octopus\Producer($config);
$rst = $producer->setBrokerServer()
                 ->setProducerTopic('qkl01')
                 ->producer('qkl037', 90);
var_dump($rst);
```RdKafka

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

### defaultDrMsg
```php
function defaultDrMsg($kafka, $message) {
    file_put_contents($this->config['log_path'] . "/dr_cb.log", var_export($message, true).PHP_EOL, FILE_APPEND);
}
```

### defaultErrorCb
```php
function defaultErrorCb($kafka, $err, $reason) {
    file_put_contents($this->config['log_path'] . "/err_cb.log", sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason).PHP_EOL, FILE_APPEND);
}
```


### defaultRebalance
```php
function defaultRebalance(\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null)
{
    switch ($err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            echo "Assign: ";
            if (is_null($this->getCurrentTopic())) {
                $kafka->assign();
            } else {
                $kafka->assign([
                    new \RdKafka\TopicPartition( $this->getCurrentTopic(), $this->getPartition($this->getCurrentTopic()), $this->getOffset($this->getCurrentTopic()) )
                ]);
            }
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            echo "Revoke: ";
            var_dump($partitions);
            $kafka->assign(NULL);
            break;

        default:
            throw new \Exception($err);
    }
}
```

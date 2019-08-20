<?php

include 'vendor/autoload.php';

// 高阶消费者
function consumer()
{
    $config = [
        'brokers' => '172.17.19.18:19092,172.17.19.18:19093,172.17.19.18:19094',
        'log_level' => LOG_DEBUG,
        'timeout' => 1,
    ];
    $consumer = new Octopus\Consumer($config);
    $consumer->setConsumerGroup('ts_high')
        ->setBrokerServer($config['brokers'])
        ->setTopic('ts')
        ->subscribe('ts')
        ->consumer(function($msg){
            print("high: partition:$msg->partition, msg:$msg->payload" . PHP_EOL);
        });
}

// 低阶消费者
function consumer2()
{
    $config = [
        'brokers' => '172.17.19.18:19092,172.17.19.18:19093,172.17.19.18:19094',
        'log_level' => LOG_DEBUG,
        'timeout' => 3,
    ];
    $consumer = new Octopus\Consumer($config);
    $consumer->setConsumerGroup('ts-low')
        ->setBrokerServer($config['brokers'])
        // 自定义设置分区，消费开始点
        ->setTopic('ts', 11)
        ->subscribe('ts', \Octopus\Consumer::LOW_LEVEL)
        ->consumer(function($msg){
            // 实体业务处理代码
//            print("low: partition:$msg->partition, msg:$msg->payload" . PHP_EOL);
            var_dump($msg);
        });

}

consumer2();
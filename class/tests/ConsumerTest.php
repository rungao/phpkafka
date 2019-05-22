<?php
use PHPUnit\Framework\TestCase;

use Octopus\Consumer;

class ConsumerTest extends TestCase
{
    /**
     * 低阶消费测试
     * @throws Exception
     * @return void
    */
    public function testLowLevelConsumer()
    {
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
        $consumer = new Consumer($config);
        $consumer->setConsumerGroup($groupId)
            ->setBrokerServer($config['brokers'])
            // 自定义设置分区，消费开始点
            ->setTopic($topic, $partitionNum, $offset)
            // 自定义C端参数设置
            ->setTopicConf('request.required.acks', -1)
            ->subscribe($topic, Consumer::LOW_LEVEL)
            ->consumer(function ($message){
                // 实体业务处理代码
                $this->assertInstanceOf(\RdKafka\Message::class, $message);
            });
    }

    /**
     * 高阶消费测试
     * @throws Exception
     * @return void
     */
    public function testHighLevelConsumer()
    {
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
            ->consumer(function ($message){
                $this->assertInstanceOf(\RdKafka\Message::class, $message);
            });
    }
}

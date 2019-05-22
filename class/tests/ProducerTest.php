<?php
use PHPUnit\Framework\TestCase;

use Octopus\Producer;

class ProducerTest extends TestCase
{
    /**
     * 生产者测试
     */
    public function testProducer()
    {
        $config = [
            // 请自行设置你的kafka地址
            // 172.17.19.18:19092
            'brokers' => 'localhost:9092',
            'log_level' => LOG_DEBUG
        ];

        $topic = 'ts_click';

        $producer = new Producer($config);

        $msg = 'kafka phpunit test';

        $rest = $producer->setBrokerServer()
            ->setProducerTopic($topic)
            ->producer($msg, 0);

        $this->assertEquals(true, $rest);
    }
}

<?php
use PHPUnit\Framework\TestCase;

use Octopus\RdKafka;

class ProducerTest extends TestCase
{
    public function testProducer()
    {
        $config = [
            // 请自行设置你的kafka地址
            // 172.17.19.18:19092
            'brokers' => '172.17.19.18:19092',
            'log_level' => LOG_DEBUG
        ];

        $topic = 'ts_click';

        $producer = new Octopus\Producer($config);

        $msg = 'kafka phpunit test';

        $rest = $producer->setBrokerServer()
            ->setProducerTopic($topic)
            ->producer($msg, 0);

        $this->assertEquals(true, $rest);
    }
}

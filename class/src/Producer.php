<?php
/**
 * Kafka封装类
 * @copyright Copyright (c) 2019 2345.com, All rights reserved.
 * @author: gaox <gaox@2345.com>
 * @package Octopus\RdKafka
 * @version  1.0.1
 */

namespace Octopus;

class Producer
{
    /**
     * @var \RdKafka\Producer
    */
    private $producer;

    /**
     * @var \RdKafka\ProducerTopic
    */
    private $producerTopic;

    /**
     * @var \RdKafka\TopicConf
    */
    private $producerTopicConf;

    /**
     * @var \RdKafka
     */
    private $rk;

    /**
     * @var \RdKafka\Conf
    */
    private $rkConf;

    /**
     * @var array $config
     */
    private  $config;

    /**
     * @var array $brokerConfig
     */
    private  $brokerConfig;

    /**
     * 初始化producer
     * @param array $config
     * @return void
    */
    public function __construct($config = [])
    {
        $this->rk = new RdKafka($config);
        $this->rkConf = $this->rk->getConf();
        $this->config = $this->rk->getConfig();
        $this->brokerConfig = $this->rk->getBrokerConfig();
    }

    /**
     * 设置broker List，多个使用逗号隔开
     * @param string $brokerList
     * @return $this
     */
    public function setBrokerServer($brokerList = NULL)
    {
        $this->producer = new \RdKafka\Producer($this->rkConf);
        $this->producer->setLogLevel($this->config['log_level'] ?: LOG_WARNING);
        /**
         * 指定 broker 地址,多个地址用"," 分割。
         * 当然，这里也可以 $rk->addBrokers("192.168.0.201:9092"); 只制定一个broker，照样可以从不同partition拿到数据，因为kafka帮我们在内部做了处理
         * 将broker list写全的目的是为了防止指定的broker宕机，如果发生宕机，那么kafka会依次查找后面的kafka，比如201宕机，就会去找202。
         * 当然你也可以先从zookeeper获取broker list，这样能保证获取的broker list都是可用的
         */
        $this->producer->addBrokers($brokerList ? : $this->config['brokers']);

        return $this;
    }

    /**
     * 设置topic的名称
     * @param string $topicName
     * @return $this
    */
    public function setProducerTopic($topicName)
    {
        $this->producerTopicConf = new \RdKafka\TopicConf();
        // -1必须等所有brokers确认 1当前服务器确认 0不确认，这里如果是0回调里的offset无返回，如果是1和-1会返回offset
        $this->producerTopicConf->set('request.required.acks', $this->brokerConfig['request.required.acks']);
        $this->producerTopic = $this->producer->newTopic($topicName, $this->producerTopicConf);

        return $this;
    }

    /**
     * 发送消息
     * @param string $msg 要发送的信息
     * @param int $partition 分区信息，RD_KAFKA_PARTITION_UA-自动分区
     * @param sting $key 消息密钥,作为主题分区使用，如根据UUID分区相同UUID就会分到一个分区
     * @return boolean
     */
    public function producer($msg, $partition = RD_KAFKA_PARTITION_UA, $key = null)
    {
        $this->producerTopic->produce($partition, 0, $msg, $key);

        $len = $this->producer->getOutQLen();
        while ($len > 0) {
            $len = $this->producer->getOutQLen();
            $this->producer->poll(50);
        }

        return true;
    }

    /**
     * @return \RdKafka
    */
    public function getKafka()
    {
        return $this->rk;
    }

    /**
     * @return \RdKafka\Conf
    */
    public function getKafkaConf()
    {
        return $this->rkConf;
    }
}

<?php
/**
 * Kafka封装类
 * @copyright Copyright (c) 2019 2345.com, All rights reserved.
 * @author: gaox <gaox@2345.com>
 * @package Octopus\RdKafka
 * @version  1.0.1
 */

namespace Octopus;

class Consumer
{
    /**
     * @var \RdKafka\KafkaConsumer $consumer
    */
    private $consumer;

    /**
     * @var \RdKafka\ConsumerTopic $consumerTopic
    */
    private $consumerTopic;

    /**
     * @var \RdKafka
     */
    private  $rk;

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
     * @var \RdKafka\TopicConf
    */
    private $topicConf;

    /**
     * @param array $config consumer配置信息
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
     * 设置消费组
     * @param string $groupName
     * @return $this
     */
    public function setConsumerGroup($groupName)
    {
        // 配置groud.id 具有相同 group.id 的consumer 将会处理不同分区的消息
        // 所以同一个组内的消费者数量如果订阅了一个topic， 那么消费者进程的数量多于 多于这个topic 分区的数量是没有意义的
        $this->rkConf->set('group.id', $groupName);
        return $this;
    }

    /**
     * 设置服务broker
     * @param  string $broker ip:port 多个使用逗号隔开
     * @return $this
     */
    public function setBrokerServer($broker)
    {
        $this->rkConf->set('metadata.broker.list', $broker);
        return $this;
    }

    /**
     * 设置服务broker
     * @param string $topicName
     * @param int $partition
     * @param int $offset
     * @return $this
     */
    public function setTopic($topicName, $partition = 0, $offset = RD_KAFKA_OFFSET_STORED)
    {
        $this->rk->setTopic($topicName, $partition, $offset);
        return $this;
    }

    /**
     * 设置consumer Topic信息
    */
    public function setConsumerTopic()
    {
        $this->topicConf = new \RdKafka\TopicConf();

        $this->topicConf->set('request.required.acks', $this->brokerConfig['request.required.acks']);
        // 在interval.ms的时间内自动提交确认、建议不要启动
        // $this->topicConf->set('auto.commit.enable', $this->brokerConfig['auto.commit.enable']);
        if ($this->brokerConfig['auto.commit.enable']) {
            $this->topicConf->set('auto.commit.interval.ms', $this->brokerConfig['auto.commit.interval.ms']);
        }

        // 设置offset的offset存储方式（none/file/broker），新版本建议由broker保存(__consumer_offsets topic)
//        $this->topicConf->set('offset.store.method', 'file');
//        $this->topicConf->set('offset.store.path', __DIR__);
        // 设置offset的存储为broker
        // $this->topicConf->set('offset.store.method', 'broker');
         $this->topicConf->set('offset.store.method', $this->brokerConfig['offset.store.method']);
        if ($this->brokerConfig['offset.store.method'] == 'file') {
            $this->topicConf->set('offset.store.path', $this->brokerConfig['offset.store.path']);
        }

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        $this->topicConf->set('auto.offset.reset', $this->brokerConfig['auto.offset.reset']);

        //设置默认话题配置
        $this->rkConf->setDefaultTopicConf($this->topicConf);

        return $this;
    }

    /**
     * 获取kafka topic，可以自定义设置一些参数信息
     * @return \RdKafka\TopicConf
    */
    public function getConsumerTopic()
    {
        return $this->topicConf;
    }

    /**
     * 订阅某个topic信息
     * @param array $topicNames
     * @return $this
     * @throws \RdKafka\Exception
     */
    public function subscribe($topicNames)
    {
        $this->consumer = new \RdKafka\KafkaConsumer($this->rkConf);
        // 让消费者订阅主题
        $this->consumer->subscribe($topicNames);
        return $this;
    }

    /**
     * 消费topic信息-高阶模式
     * 注意高阶模式需要设置setRebalanceCb回调
     * @param \Closure $handle 回调函数
     * @return void
     * @throws \RdKafka\Exception
    */
    public function consumerHighLevel(\Closure $handle)
    {
        while (true) {
            $message = $this->consumer->consume(120 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $handle($message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

    /**
     * 消费方式-低阶消费
     * 支持不同分区不同消费方式
     * @param \Closure $callback
     * @return void
    */
    public function consumerLowLevel(\Closure $callback)
    {
        $partitionNum = $this->rk->getPartition($this->rk->getCurrentTopic());

        /**
         * RD_KAFKA_OFFSET_BEGINNING 从0开始消费
         * RD_KAFKA_OFFSET_END 从队列最后开始消费
         * RD_KAFKA_OFFSET_STORED 从存储的offset开始消费
         */
        $this->consumerTopic->consumeStart(
            $partitionNum,
            $this->rk->getOffset()
        );

        while (true) {
            // 第一个参数与上面设置是冗余的，但是需要保持一致
            // 参数1表示消费分区，这里是分区0
            // 参数2表示同步阻塞多久,单位毫秒
            $message = $this->consumerTopic->consume($partitionNum, 12 * 1000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    //todo 消费
                    $callback($message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    echo $message->err . ":" . $message->errstr;
                    break;
            }
        }

    }
}
<?php
/**
 * Kafka封装类
* @copyright Copyright (c) 2019 2345.com, All rights reserved.
* @author: gaox <gaox@2345.com>
* @package Octopus\RdKafka
* @version  1.0.1
*/

namespace Octopus;

class RdKafka
{
    /**
     * @var \RdKafka\Conf $rkConf
    */
    private $rkConf;

    /**
     * @var $config \Rdkafka\Conf存储的配置信息
    */
    private $config = [];

    /**
     * @var array $brokerConfig broker的配置
    */
    private $brokerConfig = [];

    /**
     * @var string $topic 主题名称
    */
    private $topic = '';

    /**
     * 初始化的一些配置
     * @param array $config \Rdkafka\Conf的一些配置
     * @see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
     * @return void
    */
    public function __construct(array $config = [])
    {
        $defaultConfig = [
            // 默认broker list
            'brokers' => '127.0.0.1',
            // 系统默认日志存储路径
            'log_path' => sys_get_temp_dir(),
            // 消息模式
            'log_level' => LOG_DEBUG,
            // P:分发报告回调函数
            'dr_msg_cb' => [$this, 'defaultDrMsg'],
            // 发生错误回调
            'error_cb' => [$this, 'defaultErrorCb'],
            // C:消费者组重新分配后调用
            'rebalance_cb' => [$this, 'defaultRebalance']
        ];

        $brokerConfig = [
            // C:producer的ack机制（0：异步不等待反应，1：producer leader收到确认 -1：producer follower收到确认）
            // 三种机制，性能依次递减 (producer吞吐量降低)，数据健壮性则依次递增
            'request.required.acks' => -1,
            // 新版本已启用，不建议设置
            //'auto.commit.enable' => 1,
            // C:高级消费者使用，偏移提交到存储的频率
            'auto.commit.interval.ms' => 100,
            // 新版本已经弃用，不建议设置
            //'offset.store.method' => 'broker',
            // 新版本已经弃用，不建议设置
            //'offset.store.path' => sys_get_temp_dir(),
            // C:当没有初始偏移量时，从哪里开始读取('smallest': start from the beginning)
            'auto.offset.reset' => 'smallest',
        ];

        $this->config = array_merge($defaultConfig, $config);
        $this->brokerConfig = array_merge($brokerConfig, isset($config['broker']) ? $config['broker'] : []);

        $this->rkConf = new \RdKafka\Conf();

        $this->setDrMsgCb($this->config['dr_msg_cb']);
        $this->setErrorCb($this->config['error_cb']);
        $this->setRebalanceCb($this->config['rebalance_cb']);
    }

    /**
     * 获取kafka conf配置类
     * @return \RdKafka\Conf
    */
    public function getConf()
    {
        return $this->rkConf;
    }

    /**
     * 获取kafka conf配置信息数据
     * @return array
    */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * 获取kafka broker list
     * @return  array
    */
    public function getBrokerConfig()
    {
        return $this->brokerConfig;
    }

    /**
     * 设置话题topic的partition、offset
     * @param string $topicName topic名称
     * @param int $partition 分区编号
     * @param int $offset  消费开始点
     * @return void
     */
    public function setTopic($topicName, $partition = 0, $offset = 0)
    {
        $this->topics[$topicName] = $topicName;
        $this->partitions[$topicName] = $partition;
        $this->offsets[$topicName] = $offset;
        $this->topic = $topicName;
    }

    /**
     * 获取话题
     * @return mixed
     */
    public function getCurrentTopic()
    {
        return $this->topic;
    }

    /**
     * 获取话题的offset
     * @param string $topicName 名称
     * @return mixed
     */
    public function getPartition($topicName)
    {
        return $this->partitions[$topicName];
    }

    /**
     * 获取话题的offset
     * @param string $topicName 名称
     * @return mixed
     */
    public function getOffset($topicName)
    {
        return $this->offsets[$topicName];
    }

    /**
     * 分发报告回调函数
     * @param  \Closure $callback 回调函数
     * @return void
    */
    private function setDrMsgCb($callback)
    {
        if (is_null($callback)) {
            return ;
        }

        $this->rkConf->setDrMsgCb(function ($kafka, $message) use ($callback) {
            call_user_func_array($callback, [$kafka, $message]);
        });
    }

    /**
     * 设置错误后的回调方法
     * @param  \Closure $callback 回调函数
     * @return void
     */
    private function setErrorCb($callback)
    {
        if (is_null($callback)) {
            return ;
        }

        $this->rkConf->setErrorCb(function ($kafka, $err, $reason) use ($callback) {
            call_user_func_array($callback, [$kafka, $err, $reason]);
        });
    }

    /**
     * 设置balance回调
     * Kafka新设置consumer、新添加broker后都会触发kafka的rebalance回调
     * @param  \Closure $callback 回调函数
     * @return void
     */
    private function setRebalanceCb($callback)
    {
        if (is_null($callback)) {
            return ;
        }

        $this->rkConf->setRebalanceCb(function ($kafka, $err, $partitions) use ($callback) {
            call_user_func_array($callback, [$kafka, $err, $partitions]);
        });
    }

    /**
     * 系统默认分发报告回调函数
     * @param  \RdKafka $kafka Kafka
     * @param string $message 错误信息
     * @return void
     */
    private function defaultDrMsg($kafka, $message)
    {
        file_put_contents($this->config['log_path'] . "/dr_cb.log", var_export($message, true).PHP_EOL, FILE_APPEND);
    }

    /**
     * 系统默认错误方法处理
     * @param  \RdKafka $kafka Kafka类
     * @param string $err 错误信息
     * @param string $reason 错误内容
     * @return void
     */
    private function defaultErrorCb($kafka, $err, $reason)
    {
        file_put_contents($this->config['log_path'] . "/err_cb.log", sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason).PHP_EOL, FILE_APPEND);
    }

    /**
     * 系统默认的balance方法
     * @param \RdKafka\KafkaConsumer $kafka
     * @param string $err
     * @param array $partitions
     * @throws \Exception
     * @return void
     */
    private function defaultRebalance(\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null)
    {
        switch ($err) {
            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                echo "Assign: ";
                if (is_null($this->getCurrentTopic())) {
                    $kafka->assign();
                } else {
                    $kafka->assign([
                        new \RdKafka\TopicPartition(
                            $this->getCurrentTopic(),
                            $this->getPartition($this->getCurrentTopic()),
                            $this->getOffset($this->getCurrentTopic())
                        )
                    ]);
                }
                break;

            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                echo "Revoke: ,partition: $partitions";
                $kafka->assign(null);
                break;

            default:
                throw new \Exception($err);
        }
    }
}

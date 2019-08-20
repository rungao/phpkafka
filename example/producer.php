<?php
include 'vendor/autoload.php';
/**
 * @return boolean
*/
function producer($msg)
{
    $config = [
        'brokers' => '172.17.19.18:19092,172.17.19.18:19093,172.17.19.18:19094',
        'log_level' => LOG_DEBUG,
        'error_cb' => function($kafka, $message){
            var_dump($message);
        }
    ];

    $topic = 'ts';

    $producer = new Octopus\Producer($config);

    return $producer->setBrokerServer()
        ->setProducerTopic($topic)
        ->setTopicConf('request.required.acks', -1)
        ->producer($msg);
}

for ($i = 0; $i < 10; $i++) {
    print $i . PHP_EOL;
    $msg = date('Y-m-d H:i:s') . '-' . $i;
    producer($msg);
}

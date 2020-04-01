package com.xinhuanet.utils;

import com.xinhuanet.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;

/**
 * @program: BigMANService
 * @description: 读写kafka的方法类
 * @author: Mr.Young
 * @create: 2018-11-23 19:54
 **/

public class KafkaUtil2 {
    private final static Logger logger = LoggerFactory.getLogger(KafkaUtil2.class);
    private ConsumerType consumerType = null;

    public KafkaUtil2(ConsumerType type) {
        this.consumerType = type;
    }


    @Value("${bootstrap.servers}")
    String bootstrap_servers = "10.121.17.193:9092,10.121.17.194:9092,10.121.17.195:9092";
    @Value("${group.id}")
    String group_id = "1";
    @Value("${enable.auto.commit}")
    String enable_auto_commit = "false";
    @Value("${auto.commit.interval.ms}")
    String auto_commit_interval_ms = "1000";
    @Value("${max.poll.records}")
    int max_poll_records = 1000;
    @Value("${session.timeout.ms}")
    String session_timeout_ms = "30000";
    @Value("${key.deserializer}")
    String key_deserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    @Value("${value.deserializer}")
    String value_deserializer = "org.apache.kafka.common.serialization.StringDeserializer";


    /**
     * @Description: 创建消费者实例
     * @Param: []
     * @return: org.apache.kafka.clients.consumer.KafkaConsumer<java.lang.String,java.lang.String>
     * @Author: Mr.Young
     * @Date: 2019-1-16
     */
    public KafkaConsumer<String, String> genConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap_servers);
        props.put("group.id", Config.getValue("car.classify.group.id"));
        props.put("enable.auto.commit", enable_auto_commit);
        // props.put("auto.commit.interval.ms", auto_commit_interval_ms);
        // props.put("max.poll.records", max_poll_records);// 每次调用poll返回的消息数
        // props.put("session.timeout.ms", session_timeout_ms);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", key_deserializer);
        props.put("value.deserializer", value_deserializer);
        props.put("auto.offset.reset", "earliest");

        return new KafkaConsumer<String, String>(props);
    }

    /***
     * 可配置的消费者实例:热词实体词
     * @return
     */
    public KafkaConsumer<String, String> genConfConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getValue("words.bootstrap.servers"));
        props.put("group.id", Config.getValue("words.group.id"));
        props.put("enable.auto.commit", Config.getValue("words.enable.auto.commit"));
        props.put("auto.commit.interval.ms", Config.getValue("words.auto.commit.interval.ms"));
        props.put("max.poll.records", Config.getValue("words.max.poll.records"));// 每次调用poll返回的消息数
        props.put("session.timeout.ms", Config.getValue("words.session.timeout.ms"));
        props.put("key.deserializer", Config.getValue("key.deserializer"));
        props.put("value.deserializer", Config.getValue("value.deserializer"));
        props.put("auto.offset.reset", Config.getValue("words.auto.offset.reset"));

        return new KafkaConsumer<String, String>(props);
    }

    /***
     * 可配置的消费者实例:car
     * @return
     */
    public KafkaConsumer<String, String> genCarConfConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getValue("car.classify.bootstrap.servers"));
        props.put("group.id", Config.getValue("car.classify.group.id"));
        props.put("enable.auto.commit", Config.getValue("car.classify.enable.auto.commit"));
        props.put("auto.commit.interval.ms", Config.getValue("car.classify.auto.commit.interval.ms"));
        props.put("max.poll.records", Config.getValue("car.classify.max.poll.records"));// 每次调用poll返回的消息数
        props.put("session.timeout.ms", Config.getValue("car.classify.session.timeout.ms"));
        props.put("key.deserializer", Config.getValue("key.deserializer"));
        props.put("value.deserializer", Config.getValue("value.deserializer"));
        props.put("auto.offset.reset", Config.getValue("car.classify.auto.offset.reset"));
        return new KafkaConsumer<String, String>(props);
    }

    /***
     * 可配置的消费者实例:口碑
     * @return
     */
    public KafkaConsumer<String, String> genPublicPraiseConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getValue("publicpraise.bootstrap.servers"));
        props.put("group.id", Config.getValue("publicpraise.group.id"));
        props.put("enable.auto.commit", Config.getValue("publicpraise.enable.auto.commit"));
        props.put("auto.commit.interval.ms", Config.getValue("publicpraise.auto.commit.interval.ms"));
        props.put("max.poll.records", Config.getValue("publicpraise.max.poll.records"));// 每次调用poll返回的消息数
        props.put("session.timeout.ms", Config.getValue("publicpraise.session.timeout.ms"));
        props.put("key.deserializer", Config.getValue("key.deserializer"));
        props.put("value.deserializer", Config.getValue("value.deserializer"));
        props.put("auto.offset.reset", Config.getValue("publicpraise.auto.offset.reset"));
        return new KafkaConsumer<String, String>(props);
    }

    /**
     * @Description: 创建Producer实例
     * @Param: []
     * @return: org.apache.kafka.clients.producer.KafkaProducer<java.lang.String,java.lang.String>
     * @Author: Mr.Young
     * @Date: 2019-1-17
     */
    public KafkaProducer<String, String> genProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap_servers);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("compression.type", "snappy");
        properties.put("buffer.memory", 1073741824);
        properties.put("max.request.size", 1073741824);
        properties.put("request.timeout.ms", 30000000);
        properties.put("receive.buffer.bytes", 32768);
        properties.put("send.buffer.bytes", 131072);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(properties);
    }


    public void listAll(String topicName) {
        KafkaConsumer<String, String> consumer = null;
        if (this.consumerType == ConsumerType.CarClassfy) {
            // 汽车分类
            consumer = genCarConfConsumer();
        } else if (this.consumerType == ConsumerType.HotNerWord) {
            //实体词热词
            consumer = genConfConsumer();
        } else {
            // 口碑
            consumer = genPublicPraiseConsumer();
        }
        consumer.subscribe(Collections.singletonList(topicName));
        try {
            while (true) {  //1)
                ConsumerRecords<String, String> records = consumer.poll(100);  //2)
                for (ConsumerRecord<String, String> record : records)  //3)
                {
                    logger.info("{}", record.value());
                }
                // logger.info("位移提交");
                // consumer.commitSync();
            }
        } finally {
            consumer.close(); //4
            logger.info("消费者关闭！");
        }
    }

    /**
     * 取回口碑数据
     *
     * @param topicName
     * @param taskID
     * @return
     */
    public String getPraiseByTaskId(String topicName, String taskID) {
        KafkaConsumer<String, String> consumer = null;
        if (this.consumerType == ConsumerType.CarClassfy) {
            // 汽车分类
            consumer = genCarConfConsumer();
        } else if (this.consumerType == ConsumerType.HotNerWord) {
            //实体词热词
            consumer = genConfConsumer();
        } else {
            // 口碑
            consumer = genPublicPraiseConsumer();
        }
        consumer.subscribe(Collections.singletonList(topicName));
        logger.info("口碑topicName={}正在取回kafka数据...", topicName);
        int count = 100;
        try {
            while (true) {  //1)
                if ((count--) <= 0) {
                    logger.info("口碑服务=========>>>取不到kafka数据");
                    return null;
                }
                int time = 1000*60*15; //分钟
                ConsumerRecords<String, String> records = consumer.poll(time);  //2)
                for (ConsumerRecord<String, String> record : records)  //3)
                {
                    int index = record.value().indexOf(":");
                    String taskIDKakfa = record.value().substring(0, index);
                    if (taskID.equals(taskIDKakfa)) {
                        logger.info("口碑服务=============>>>取到taskID={}<<<==========", taskID);
                        return record.value().substring(index + 1);
                    }
                }
                // logger.info("位移提交");
                // consumer.commitSync();
            }
        } finally {
            consumer.close(); //4
            logger.info("消费者关闭！");
        }
    }


    public String getValueByTaskId(String topicName, String taskID) {
        String serverName = "";
        KafkaConsumer<String, String> consumer;
        if (this.consumerType == ConsumerType.CarClassfy) {
            // 汽车分类
            serverName = "汽车分类";
            consumer = genCarConfConsumer();
        } else if (this.consumerType == ConsumerType.HotNerWord) {
            //实体词热词
            consumer = genConfConsumer();
            serverName="实体词热词";
        } else {
            // 口碑
            serverName="口碑";
            consumer = genPublicPraiseConsumer();
        }
        // 指定主题
        consumer.subscribe(Collections.singletonList(topicName));
        logger.info("topicName={}，taskID={}正在取回kafka数据...", topicName, taskID);
        int count = 100;
        try {
            while (true) {  //1)
                if ((count--) <= 0) {
                    logger.info("{}=========>>>取不到kafka数据", serverName);
                    return null;
                }
                ConsumerRecords<String, String> records = consumer.poll(60000);  //2)
                logger.info("recods size: ", records.count());
                for (ConsumerRecord<String, String> record : records)  //3)
                {
                    if (taskID.equals(record.key())) {
                        logger.info("{}=========>>>取到kafka数据：{}", serverName, record.value());
                        return record.value();
                    }
                }
                // logger.info("位移提交");
                // consumer.commitSync();
            }
        } finally {
            consumer.close(); //4
            logger.info("消费者关闭！");
        }
    }

    /**
     * 热词实体词，获取所有历史消息
     *
     * @return
     */
    public List<String> getHotWordNerMessage() {
        KafkaConsumer<String, String> consumer = genConfConsumer();
        String topicName = Config.getValue("topic.name");
        consumerSeekback(consumer, topicName);
        List recordList = getMessage(consumer);
        return recordList;
    }

    /**
     * 获取所有历史消息
     *
     * @param topicName
     * @return
     */
    public List<String> getConfMessage(String topicName) {
        KafkaConsumer<String, String> consumer = genConfConsumer();
        consumerSeekback(consumer, topicName);
        List recordList = getMessage(consumer);
        return recordList;
    }

    /**
     * @Description: 获取所有的历史消息
     * @Param: [topicName topic名称]
     * @return: java.util.List<java.lang.String> 所有的消息
     * @Author: Mr.Young
     * @Date: 2018-11-23
     */
    public List<String> getMessage(String topicName) {
        KafkaConsumer<String, String> consumer = genConsumer();
        consumerSeekback(consumer, topicName);
        List recordList = getMessage(consumer);
        return recordList;
    }

    public Map<String, String> getMessageByKey(String topicName, String key) {
        KafkaConsumer<String, String> consumer = genConsumer();
        consumerSeekback(consumer, topicName);
        Map<String, String> message = getMessageByKey(consumer, key);
        return message;
    }

    public void consumerSeekback(KafkaConsumer<String, String> consumer, String topicName) {
        logger.info("seek back .......");
        consumer.subscribe(Collections.singletonList(topicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                Map<TopicPartition, Long> beginningOffset = consumer.beginningOffsets(partitions);
                for (Map.Entry<TopicPartition, Long> entry : beginningOffset.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    long offset = entry.getValue();
                    consumer.seek(tp, offset);
                }
//                consumer.seekToBeginning(partitions);
            }
        });
    }

    public List<String> getMessage(KafkaConsumer<String, String> consumer) {
        List recordList = new ArrayList();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            logger.info("records.size()=" + records.count());
            if (records.count() > 0) {
                for (ConsumerRecord<String, String> record : records) {
                    recordList.add(record.value());
                    consumer.commitAsync();
                }
                consumer.close();
                break;
            }
        }
        return recordList;
    }

    public Map<String, String> getMessageByKey(KafkaConsumer<String, String> consumer, String key) {
        Map<String, String> message = new HashMap<String, String>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            boolean flag = false;
            logger.info("records.size()=" + records.count());
            if (records.count() > 0) {
                for (ConsumerRecord<String, String> record : records) {
                    consumer.commitAsync();
                    if (record.key().equals(key)) {
                        message.put(key, record.value());
                        flag = true;
                    }
                }
                if (flag) {
                    consumer.close();
                    break;
                }
            }
        }
        return message;
    }
}

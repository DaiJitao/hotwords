package hot.words.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

/**
 * @program: BigMANService
 * @description: 读写kafka的方法类
 * @author: Mr.Young
 * @create: 2018-11-23 19:54
 **/

public class KafkaUtil {
    private final static Logger logger = LoggerFactory.getLogger(KafkaUtil.class);

    String bootstrap_servers = Config.getValue("hotwords.bootstrap.servers");
    String group_id = Config.getValue("hotwords.group.id");
    String enable_auto_commit = Config.getValue("hotwords.enable.auto.commit");
    String auto_commit_interval_ms = Config.getValue("hotwords.auto.commit.interval.ms");
    int max_poll_records = Integer.parseInt(Config.getValue("hotwords.max.poll.records"));
    String session_timeout_ms = Config.getValue("hotwords.session.timeout.ms");
    String key_deserializer = Config.getValue("key.deserializer");
    String value_deserializer = Config.getValue("value.deserializer");
    String offset_reset = Config.getValue("hotwords.auto.offset.reset");




    /***
     * @return
     */
    public KafkaConsumer<String, String> genConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap_servers);
        props.put("group.id", group_id);
        props.put("enable.auto.commit", enable_auto_commit);
        props.put("auto.commit.interval.ms",auto_commit_interval_ms);
        props.put("max.poll.records",max_poll_records);// 每次调用poll返回的消息数
        props.put("session.timeout.ms", session_timeout_ms);
        props.put("auto.offset.reset", offset_reset);
        props.put("key.deserializer", key_deserializer);
        props.put("value.deserializer", value_deserializer);

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
        properties.put("key.serializer", Config.getValue("key.serializer"));
        properties.put("value.serializer", Config.getValue("value.serializer"));

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) {
        String topic = Config.getValue("hotwords.topic.name");
        String key = "name";
        String value = "daijitao123";
        KafkaUtil kafkaUtil = new KafkaUtil();
        kafkaUtil.send(topic, key,value);
        System.out.println(kafkaUtil.getValueByTaskId(topic, key));

    }

    public void send(String topicName, String key, String value) {
        KafkaProducer<String, String> producer = genProducer();
        try {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
            // 异步发送消息
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        new Exception(e);
                    } else {
                        logger.info("kafka消息发送成功！{}",recordMetadata);
                        System.out.println("kafka消息发送成功！" + value + " " + recordMetadata);
                    }
                }
            });
            producer.flush();
        } catch (Exception e) {
            System.out.println(e);
            logger.error("消息发送错误！", e);
        } finally {
            producer.close();
        }
    }


    /**
     * @param topicName
     * @param taskID
     * @return
     */
    public String getPraiseByTaskId(String topicName, String taskID) {
        KafkaConsumer<String, String> consumer = genConsumer();

        consumer.subscribe(Collections.singletonList(topicName));
        logger.info("口碑topicName={}正在取回kafka数据...", topicName);
        int count = 100;
        try {
            while (true) {  //1)
                if ((count--) <= 0) {
                    logger.info("口碑服务=========>>>取不到kafka数据");
                    return null;
                }
                int time = 1000 * 60 * 15; //分钟
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
        }
    }


    public String getValueByTaskId(String topicName, String taskID) {
        KafkaConsumer<String, String> consumer = genConsumer();
        // 指定主题
        consumer.subscribe(Collections.singletonList(topicName));
        logger.info("topicName={}，taskID={}正在取回kafka数据...", topicName, taskID);
        System.out.println("正在取回kafka数据...");
        int count = 100;
        try {
            while (true) {  //1)
                if ((count--) <= 0) {
                    logger.info("热词服务=========>>>取不到kafka数据");
                    return null;
                }
                ConsumerRecords<String, String> records = consumer.poll(60000);  //2)
                logger.info("recods size: ", records.count());
                System.out.println("recods size: " + records.count());
                for (ConsumerRecord<String, String> record : records)  //3)
                {
                    System.out.println(record.key());
                    if (taskID.equals(record.key())) {
                        logger.info("热词服务=========>>>取到kafka数据：{}",record.value());
                        System.out.println(record.value());
                        return record.value();
                    }
                }
                // logger.info("位移提交");
                // consumer.commitSync();
            }
        } finally {
            consumer.close(); //4
        }
    }

}

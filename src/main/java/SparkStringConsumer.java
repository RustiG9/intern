import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;


public class SparkStringConsumer extends Thread {
    private static int min;
    private static int max;
    private static int size = 0;
    private static int time;
    private static int count = 0;

    public static void setTime(int time) {
        SparkStringConsumer.time = time;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public SparkStringConsumer(int min, int max, int time) {
        this.min = min;
        this.max = max;
        this.time = time;
    }

    public void run() {
//    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        SparkConf conf = new SparkConf()
                .setAppName("kafka-test")
                .setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(time*60000));

        Set<String> topics = Collections.singleton("test");
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                size += Integer.parseInt(record.value());
            });
            if (min > size || size > max) {
                kafkaProducer.send(new ProducerRecord("alerts", Integer.toString(count++), Integer.toString(size) + " " + new Date()));
                System.out.println(min + "=i, m=" + max);
                System.out.println("ne normalno");
                System.out.println(size);
            } else {
                System.out.println(min + "=i, m=" + max);
                System.out.println("vse normalno");
                System.out.println(size);
            }
            size = 0;
        });
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            ssc.close();
        }
    }
}

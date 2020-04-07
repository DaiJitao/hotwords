package hot.words.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by dell on 2020/4/7.
 */
public class StreamingTest {

    public static void main(String[] args) throws InterruptedException {
        fsFileStream();
    }

    public static void fsFileStream() throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setAppName("StreamingApp").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));

        //
        String loaclFile = "file:///F:/data/hotwordner";
        JavaDStream<String> files = jsc.textFileStream("hdfs://carbigdata1:9000/amend/hotwordNer/taskDir/");
        files.print();

        jsc.start();
        jsc.awaitTermination();

        jsc.stop();
    }

    /**
     * yum install socket
     * @throws InterruptedException
     */
    public static void socketTextStream() throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setAppName("StreamingApp").setMaster("local[2]");
//        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
//        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("carbigdata1", 9999);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> t =  Arrays.asList(s.split(" "));
                return t.iterator();
            }
        });

        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        counts.print();

        jsc.start();
        jsc.awaitTermination();

        jsc.stop();
    }
}

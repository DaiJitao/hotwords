package hot.words.service;

import breeze.linalg.Axis;
import com.hankcs.hanlp.seg.common.Term;
import com.sun.imageio.plugins.common.I18N;
import hot.words.utils.HanlpUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Int;
import scala.Tuple2;

import java.util.*;

public class Demo extends KeyWordsExtractor {


    public static void main3(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("flatmap");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("w1 1", "w2 2", "w3 3", "w4 4");

        JavaRDD<String> listRdd = sc.parallelize(list);
        JavaPairRDD<String, String> pairRdd = listRdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>("fisrt second 33 44 55 66 last", "1 2 3 4 5 6 7");
            }
        });

        System.out.println(pairRdd.collect());
        JavaPairRDD<String, String> words_weights = pairRdd.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> tuple2) throws Exception {
                String[] s1 = tuple2._1.split(" ");
                String[] s2 = tuple2._2.split(" ");

                List<Tuple2<String,String>> list1 = new ArrayList<>();
                for (int i = 0; i < s1.length; i++) {
                    list1.add(new Tuple2<>(s1[i],s2[i]));

                }
                return list1.iterator();
            }
        });

        System.out.println(words_weights.collect());


        JavaPairRDD<String, Double> pairWeightRDD = listRdd.flatMapToPair(new PairFlatMapFunction<String, String, Double>() {
            @Override
            public Iterator<Tuple2<String, Double>> call(String s) throws Exception {
                return null;
            }
        });

//        JavaPairRDD<String, Integer> result = listRdd.flatMapToPair(
//                new FlatMapFunction<String, Integer>() {
//                    @Override
//                    public Iterator<Integer> call(String s) throws Exception {
//                        return null;
//                    }
//                });


    }

    public static void main2(String[] args) {
        List<String> docs = HanlpUtil.loadExampleData();
        System.out.println(docs.get(0));
        List<Term> termList = standardTokenizer.segment(docs.get(0));
        System.out.println(termList);
    }


    public static void main1(String[] args) {
        SparkConf conf = new SparkConf();


        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setAppName("DemoApp").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        final JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello word", "hi"));
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        System.out.println("并行度");
        System.out.println(lines.getNumPartitions());
        System.out.println(words.getNumPartitions());

        System.out.println(words.take(1));
        System.out.println(words.first());
        System.out.println(words.count());
        System.out.println(words.collect());
        System.out.println(words.countByValue());

        JavaDoubleRDD result = words.mapToDouble(new DoubleFunction<String>() {
            public double call(String s) throws Exception {
                return 1.0;
            }
        });
        result.persist(StorageLevel.DISK_ONLY());
        result.unpersist();
        System.out.println(result.collect());

        PairFunction<String, String, Integer> keyData = new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String words) throws Exception {
                return new Tuple2<String, Integer>(words, 1);
            }
        };
        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(keyData);
        pairRDD.coalesce(2);
        System.out.println("pairRDD num partition:" + pairRDD.getNumPartitions());
        System.out.println(pairRDD.collect().get(0));

        String data = "{\"address\":\"address here\", \"band\":\"40m\",\"callsign\":\"KK6JLK\",\"city\":\"SUNNYVALE\",\n" +
                "\"contactlat\":\"37.384733\",\"contactlong\":\"-122.032164\",\n" +
                "\"county\":\"Santa Clara\",\"dxcc\":\"291\",\"fullname\":\"MATTHEW McPherrin\",\n" +
                "\"id\":57779,\"mode\":\"FM\",\"mylat\":\"37.751952821\",\"mylong\":\"-122.4208688735\"}";

        sc.close();
    }
}

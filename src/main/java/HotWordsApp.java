
import com.alibaba.fastjson.JSONObject;
import hot.words.service.TextRankKeyWords;
import hot.words.service.TfIDFKeyWords;
import hot.words.utils.Config;
import hot.words.utils.FileUtil;
import hot.words.utils.TextCleaner;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by daijitao on 2020/3/26.
 */
public class HotWordsApp {

    private static String taskDir = Config.getValue("fs.defaultFS") + Config.getValue("taskDir");

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setAppName("热词识别（Java-version）");

        //conf.setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        sc.setLogLevel("ERROR");


        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5)); // 批次容量10秒
//
//        List<String> docs = new ArrayList<>();
//        try {
//            // 加载数据
//            //  List<String> docs = HanlpUtil.loadExampleData();
//            docs = FileUtil.loadDataFromJSON("C:\\Users\\Administrator\\Desktop\\新华网\\实体词热词\\联调\\hotwordner_0660486d0dd04325aed8926d4f1d4cd9_20200415164106.txt");
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        //  taskDir =  "hdfs://carbigdata5:9000/amend/hotwordNer/taskDir/";
        JavaDStream<String> javaDStream = jssc.textFileStream(taskDir);// 监控任务目录

        // Accumulator<Integer> vocabSize = sc.intAccumulator(0, "vocabSize");
        // 根据taskID 创建pairRDD
        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(line); //
                String taskId = jsonObject.getString("taskId");
                String data = jsonObject.getString("content");
                String rs = TextCleaner.cleanText(data);//清理文本
                // vocabSize.add(rs.length());
                return new Tuple2<>(taskId, rs);
            }
        };

        javaDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {

                JavaPairRDD<String, String> pairRDD = stringJavaRDD.mapToPair(keyData);
                JavaRDD<String> keysRDD = pairRDD.keys().distinct(); //
                keysRDD.persist(StorageLevel.MEMORY_AND_DISK());
                List<String> keys = keysRDD.collect();
                if (keys.size() > 0) {
                    System.out.println("keys ===========>" + keys.toString());
                }
                for (String key : keys) {
                    List<String> docs = pairRDD.lookup(key);
                    System.out.println("===========>开始计算时间：" + System.currentTimeMillis() + " taskID=" + key);
                    getResult(sc, spark, docs, key);
                }
            }
        });


        jssc.start();
        jssc.awaitTermination();
        jssc.stop(false);
        jssc.close();
        sc.close();
        spark.close();

    }


    public static void getResult(JavaSparkContext sc, SparkSession spark, List<String> cleanedDocs, String taskId) {
        TextRankKeyWords textRankKeyWords = new TextRankKeyWords(); // textRank 算法
        TfIDFKeyWords tfIDFKeyWords = new TfIDFKeyWords(); // tf-idf 算法
        Map<String, Float> textRankMap = textRankKeyWords.getKeyWords(sc, cleanedDocs);
        Broadcast<Integer> sizeBrodcast = sc.broadcast(5000000);
        JavaRDD<String> cleanedDocsRDD = sc.parallelize(cleanedDocs).repartition(200);
        Map<String, Float> tfIDFMap = tfIDFKeyWords.getKeyWords(spark, sizeBrodcast, cleanedDocsRDD);
        // textRanK 和tf-df融合,提取topN
        Map<String, Float> resultMap = textRankKeyWords.top(-1, tfIDFMap, textRankMap);
        //
        System.out.println("===========>结束时间：" + System.currentTimeMillis() + "taskId=" + taskId + "\n" + resultMap.toString());

    }

    public void localMain(String[] args) {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setAppName("热词识别（Java-version）").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Accumulator<Integer> vocabSize = sc.intAccumulator(0, "vocabSize");
        // int vocabSize = 3000000;
        // 向量维度

        List<String> docs = new ArrayList<>();
        try {
            // 加载数据
            //  List<String> docs = HanlpUtil.loadExampleData();
            docs = FileUtil.loadDataFromJSON("C:\\Users\\Administrator\\Desktop\\新华网\\实体词热词\\联调\\hotwordner_0660486d0dd04325aed8926d4f1d4cd9_20200415164106.txt");

        } catch (Exception e) {
            e.printStackTrace();
        }

        JavaRDD<String> docsRDD = sc.parallelize(docs);
        // clean text
        JavaRDD<String> cleanedDocsRDD = docsRDD.map(new Function<String, String>() {

            @Override
            public String call(String doc) throws Exception {
                // vocabSize.add(doc.length());
                String rs = TextCleaner.cleanText(doc);
                vocabSize.add(rs.length());
                return rs;
            }
        });
        cleanedDocsRDD.persist(StorageLevel.MEMORY_AND_DISK());
        List<String> cleanedDocs = cleanedDocsRDD.collect();
        // cleanedDocs.forEach(System.out::println);
        System.out.println("累加器：" + vocabSize.value());
        try {
            TextRankKeyWords textRankKeyWords = new TextRankKeyWords(); // textRank 算法
            TfIDFKeyWords tfIDFKeyWords = new TfIDFKeyWords(); // tf-idf 算法
            Map<String, Float> textRankMap = textRankKeyWords.getKeyWords(sc, cleanedDocs);
            // Map<String, Float> tfIDFMap = tfIDFKeyWords.getKeyWords(spark, sizeBrodcast, docsRDD, vocabSize);
            Broadcast<Integer> sizeBrodcast = sc.broadcast(vocabSize.value());
            Map<String, Float> tfIDFMap = tfIDFKeyWords.getKeyWords(spark, sizeBrodcast, cleanedDocsRDD);
            // textRanK 和tf-df融合,提取topN
            Map<String, Float> resultMap = textRankKeyWords.top(2000, tfIDFMap, textRankMap);
            resultMap.forEach((k, v) -> System.out.println(k + "=" + v));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.close();
            spark.close();
        }
    }
}

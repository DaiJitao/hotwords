import com.alibaba.fastjson.JSONObject;
import hot.words.utils.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by daijitao on 2020/3/26.
 */
public class HotWordsAppTest {

    public void main1(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setMaster("local[4]");
        conf.setAppName("DemoApp");

        // 检查点机制,配置一个可以从错误中恢复的驱动器程序
        String checkpoint = "hdfs://carbigdata5:9000/amend/hotwordNer/tempDir";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkpoint, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                JavaSparkContext sc = new JavaSparkContext(conf);
                JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(3));
                jssc.checkpoint(checkpoint);
                return jssc;
            }
        });

//        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
//        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.checkpoint(checkpoint);
        // 监听文件流
        String file = "hdfs://carbigdata5:9000/amend/hotwordNer/taskDir";
        JavaDStream<String> javaDStream = jsc.textFileStream(file);


        javaDStream.print();


        jsc.start();
        jsc.awaitTermination();
        jsc.stop(false);
        jsc.close();


    }

    public static void main2(String[] args) throws InterruptedException {
        // socketFile();
        monitorHDFS(args);
    }

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setAppName("热词识别（Java-version）");
        conf.setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        List<String> docs = new ArrayList<>();
        try {
            // 加载数据
            //  List<String> docs = HanlpUtil.loadExampleData();
            String file = "C:\\Users\\dell\\Desktop\\upload\\hotwordner_0660486d0dd04325aed8926d4f1d4cd9_20200415164106.txt";
            docs = FileUtil.loadJSONData(file);

        } catch (Exception e) {
            e.printStackTrace();
        }

        // 根据taskID 创建pairRDD
        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(line); //
                String taskId = jsonObject.getString("taskId");
                String data = jsonObject.getString("content");
                return new Tuple2<>(taskId, data);
            }
        };

        try {
            JavaRDD<String> docsRDD = sc.parallelize(docs);

            // docs.forEach(System.out::println);

            JavaPairRDD<String, String> pairRDD = docsRDD.mapToPair(keyData);


            List<Tuple2<String, String>> r = pairRDD.collect();
            JavaRDD<String> keys = pairRDD.keys().distinct();


            System.out.println(keys.toString());

            List<String> strings = pairRDD.lookup("hotwordner_0660486d0dd04325aed8926d4f1d4cd9_20200415164106");
            System.out.println(strings);
            for (String v : strings
                    ) {
                System.out.println(v);
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.close();
            spark.close();
        }
    }


    /**
     * @throws InterruptedException
     */
    public void socketFile() throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        // conf.setMaster("local[4]");
        conf.setAppName("热词识别服务（测试）");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        // jssc.fileStream()

        // 服务端 nc -lk 9999
        JavaDStream<String> lines = jssc.socketTextStream("carbigdata1", 9999);

        JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.contains("1");
            }
        });
        errorLines.print();


        jssc.start();
        jssc.awaitTermination();
        jssc.stop(false);
        jssc.close();
    }

    /**
     * hadoop文件系统
     *
     * @throws InterruptedException
     */
    public static void monitorHDFS(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        // conf.setMaster("local[4]");
        conf.setAppName("热词识别服务（测试）");


        // 检查点机制
        //String checkpoint = "hdfs://carbigdata5:9000/amend/hotwordNer/tempDir/";
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        //jsc.checkpoint(checkpoint);
        // 监听文件流
        String file = "hdfs://amend/hotwordNer/taskDir/";
        JavaDStream<String> javaDStream = jsc.textFileStream(file);

        System.out.println("读取到数据：");
        javaDStream.print();
        System.out.println("\n");

        jsc.start();
        jsc.awaitTermination();
        jsc.stop(false);
        jsc.close();
    }
}

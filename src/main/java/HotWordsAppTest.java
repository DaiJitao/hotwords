import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by daijitao on 2020/3/26.
 */
public class HotWordsAppTest {

    public static void main1(String[] args) throws InterruptedException {
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

    public static void main(String[] args) throws InterruptedException {
        socketFile();
    }

    /**
     *
     * @throws InterruptedException
     */
    public static void socketFile() throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setMaster("local[4]");
        conf.setAppName("DemoApp");

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
     * @throws InterruptedException
     */
    public static void monitorHDFS() throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setMaster("local[4]");
        conf.setAppName("DemoApp");


        // 检查点机制
        //String checkpoint = "hdfs://carbigdata5:9000/amend/hotwordNer/tempDir/";
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        //jsc.checkpoint(checkpoint);
        // 监听文件流
        String file = "hdfs://carbigdata5:9000/amend/hotwordNer/taskDir/";
        JavaDStream<String> javaDStream = jsc.textFileStream(file);


        javaDStream.print(10);


        jsc.start();
        jsc.awaitTermination();
        jsc.stop(false);
        jsc.close();
    }
}

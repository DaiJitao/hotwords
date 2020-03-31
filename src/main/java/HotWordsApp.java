import com.hankcs.hanlp.mining.word.TfIdf;
import hot.words.service.TextRankKeyWords;
import hot.words.service.TfIDFKeyWords;
import hot.words.utils.HanlpUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

/**
 * Created by dell on 2020/3/26.
 */
public class HotWordsApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setAppName("DemoApp").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        int vocabSize = 1000;
        // 向量维度
        Broadcast<Integer> sizeBrodcast = sc.broadcast(vocabSize);
        // 加载数据
        List<String> docs = HanlpUtil.loadExampleData();
        JavaRDD<String> docsRDD = sc.parallelize(docs);

        try {
            TextRankKeyWords textRankKeyWords = new TextRankKeyWords();
            TfIDFKeyWords tfIDFKeyWords = new TfIDFKeyWords();
            Map<String, Float> textRankMap = textRankKeyWords.getKeyWords(sc, docs);
            Map<String, Float> tfIDFMap = tfIDFKeyWords.getKeyWords(spark, sizeBrodcast, docsRDD, vocabSize);
            // textRanK 和tf-df融合,提取topN
            Map<String, Float> resultMap = textRankKeyWords.top(10, tfIDFMap, textRankMap);
            System.out.println(resultMap);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.close();
            spark.close();
        }
    }
}

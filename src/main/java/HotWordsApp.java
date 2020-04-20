
import hot.words.service.TextRankKeyWords;
import hot.words.service.TfIDFKeyWords;
import hot.words.utils.FileUtil;
import hot.words.utils.TextCleaner;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by daijitao on 2020/3/26.
 */
public class HotWordsApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.port.maxRetries", "512");
        conf.setAppName("DemoApp").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Accumulator<Integer> vocabSize =  sc.intAccumulator(0, "vocabSize");
        // int vocabSize = 3000000;
        // 向量维度

        List<String> docs = new ArrayList<>();
        try{
            // 加载数据
            //  List<String> docs = HanlpUtil.loadExampleData();
         docs = FileUtil.loadDataFromJSON("C:\\Users\\Administrator\\Desktop\\新华网\\实体词热词\\联调\\hotwordner_0660486d0dd04325aed8926d4f1d4cd9_20200415164106.txt");

        }  catch (Exception e){
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
        System.out.println("累加器："+ vocabSize.value());
        try {
            TextRankKeyWords textRankKeyWords = new TextRankKeyWords(); // textRank 算法
            TfIDFKeyWords tfIDFKeyWords = new TfIDFKeyWords(); // tf-idf 算法
            Map<String, Float> textRankMap = textRankKeyWords.getKeyWords(sc, cleanedDocs);
            // Map<String, Float> tfIDFMap = tfIDFKeyWords.getKeyWords(spark, sizeBrodcast, docsRDD, vocabSize);
            Broadcast<Integer> sizeBrodcast = sc.broadcast(vocabSize.value());
            Map<String, Float> tfIDFMap = tfIDFKeyWords.getKeyWords(spark, sizeBrodcast, cleanedDocsRDD);
            // textRanK 和tf-df融合,提取topN
            Map<String, Float> resultMap = textRankKeyWords.top(2000, tfIDFMap, textRankMap);
            resultMap.forEach((k,v)->System.out.println(k + "=" + v));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.close();
            spark.close();
        }
    }
}

package hot.words.service;


import hot.words.utils.HanlpUtil;
import hot.words.utils.TextCleaner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.Row;
import scala.Tuple2;


import java.util.*;

/**
 * http://spark.apache.org/docs/2.3.1/ml-features.html
 */
public class TfIDFKeyWords extends KeyWordsExtractor {

    public Map<String, Float> getKeyWords(SparkSession spark, Broadcast<Integer> sizeBrodcast, JavaRDD<String> docsRDD, int size) {
        // 分词
        JavaRDD<String> cutWordsRDD = docsRDD.map(new Function<String, String>() {
            @Override
            public String call(String doc) throws Exception {
                String docCleaned = TextCleaner.cleanSpecialCharactersText(doc);
                //NLP分词,去除停用词
                return TextCleaner.delStopWords(nlpTokenizer.segment(docCleaned));
            }
        });
        // System.out.println("cutWordsRDD:\n" + cutWordsRDD.collect());

        // 转换为DataSet
        StructType schema = new StructType(
                new StructField[]{
                        new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
                }
        );
        JavaRDD<Row> cutWordsRowJavaRDD = cutWordsRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String doc) throws Exception {
                return RowFactory.create(doc);
            }
        });

        Dataset<Row> sentenceData = spark.createDataFrame(cutWordsRowJavaRDD, schema);
        // 句子拆分为单词；
        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(sentenceData);  // 句子拆分为单词；

        /**
         * 模型配置
         * */
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("words")
                .setOutputCol("rowFeatures")
                .setVocabSize(sizeBrodcast.getValue()) // .setMinDF(2)   // 向量长度
                .fit(wordsData); // 数据拟合,返回模型

        // 本地语料库
        Dataset<Row> featurizedData = cvModel.transform(wordsData);

        /**
         * 数据说明
         * +----------------------------------------------------+----------------------------------------+
         |rowFeatures                                         |words                                   |
         +----------------------------------------------------+----------------------------------------+
         |(15,[0,1,2,6,9,10,12],[2.0,2.0,1.0,1.0,1.0,1.0,1.0])|[你好, 我, 听过, spark, 而且, 我, 非常, 喜欢, spark]|
         一个14代表有15个单词;
         */
        // featurizedData.select("rowFeatures", "words").show(false);
        featurizedData.persist(StorageLevel.MEMORY_AND_DISK());

        // 使用本地语料库进行idf模型训练
        IDF idf = new IDF().setInputCol("rowFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData); // 建立模型

        Dataset<Row> rescaledData = idfModel.transform(featurizedData); // 数据转化
        // (99,[97,98],[0.6931471805599453,0.6931471805599453])
        Dataset<Row> resultFeatures = rescaledData.select("features"); // 得出计算结果
        JavaRDD<Row> rowJavaRDD = resultFeatures.toJavaRDD();
        // resultFeatures.show(false);

        /**
         *  id转换为单词
         */
        PairFunction<Row, String, String> keyData = new PairFunction<Row, String, String>() {
            public Tuple2<String, String> call(Row row) throws Exception {
                // (99,[97,98],[0.6931471805599453,0.6931471805599453])
                String tmp = row.toString();
                int startIndex = tmp.indexOf(",");
                int endIndex = tmp.indexOf("])");
                // 字符截取 2,5,8,13],[0.28768207245178085,0.6931471805599453,0.6931471805599453,0.6931471805599453
                String substring = tmp.substring(startIndex + 2, endIndex);
                String[] id_wieght = substring.split("],\\[");
                String ids = id_wieght[0]; // 2,5,8,13
                String wieghts = id_wieght[1]; //  0.28768207245178085,0.6931471805599453,0.6931471805599453,0.6931471805599453
                return new Tuple2<String, String>(ids, wieghts);
            }
        };

        JavaPairRDD<String, String> ids_weightsRDD = rowJavaRDD.mapToPair(keyData);
        String[] vocabulary = cvModel.vocabulary();

        JavaPairRDD<String, String> words_weights = ids_weightsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> tuple2) throws Exception {
                String[] s1 = tuple2._1.split(","); // 单词id
                String[] s2 = tuple2._2.split(","); // 权重

                List<Tuple2<String, String>> list1 = new ArrayList<>();
                for (int i = 0; i < s1.length; i++) {
                    String word = vocabulary[Integer.parseInt(s1[i])];
                    list1.add(new Tuple2<>(word, s2[i]));
                }
                return list1.iterator();
            }
        });

        // 结果
        List<Tuple2<String, String>> resultList = words_weights.collect();
        // 结果
        Map<String, Float> wordsWeightsMap = new HashMap<>(resultList.size());
        for (Tuple2<String, String> tuple2 : resultList) {
            wordsWeightsMap.put(tuple2._1, Float.parseFloat(tuple2._2));
        }
        return wordsWeightsMap;
    }


    public static void main3(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("TestSparkApp");
        conf.setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        String t = "],[";


        // 每一个句子代表一个文档
        List<Row> dataCorpus = Arrays.asList(
                RowFactory.create(0.0, "你好 我 听过 spark 而且 我 非常 喜欢 spark"),
                RowFactory.create(0.0, "我 希望 Java 可以 使用 spark"),
                RowFactory.create(1.0, "逻辑回归 模型 非常 简洁"),
                RowFactory.create(1.0, "法国 卫生部 中国 订购 总共 10亿 口罩"),
                RowFactory.create(1.0, "随着 美国 新冠病毒 大规模 爆发 求购 口罩 登天 还难"),
                RowFactory.create(1.0, "逻辑回归 模型 非常 简洁"),
                RowFactory.create(1.0, "法国 卫生部 中国 订购 总共 10亿 口罩"),
                RowFactory.create(1.0, "随着 美国 新冠病毒 大规模 爆发 求购 口罩 登天 还难"),
                RowFactory.create(1.0, "逻辑回归 模型 非常 简洁"),
                RowFactory.create(1.0, "法国 卫生部 中国 订购 总共 10亿 口罩"),
                RowFactory.create(1.0, "随着 美国 新冠病毒 大规模 爆发 求购 口罩 登天 还难"),
                RowFactory.create(1.0, "逻辑回归 模型 非常 简洁"),
                RowFactory.create(1.0, "法国 卫生部 中国 订购 总共 10亿 口罩"),
                RowFactory.create(1.0, "随着 美国 新冠病毒 大规模 爆发 求购 口罩 登天 还难"),
                RowFactory.create(1.0, "逻辑回归 模型 非常 简洁"),
                RowFactory.create(1.0, "法国 卫生部 中国 订购 总共 10亿 口罩"),
                RowFactory.create(1.0, "随着 美国 新冠病毒 大规模 爆发 求购 口罩 登天 还难"),
                RowFactory.create(1.0, "逻辑回归 模型 非常 简洁"),
                RowFactory.create(1.0, "法国 卫生部 中国 订购 总共 10亿 口罩"),
                RowFactory.create(1.0, "随着 美国 新冠病毒 大规模 爆发 求购 口罩 登天 还难"),
                RowFactory.create(1.0, "逻辑回归 模型 非常 简洁"),
                RowFactory.create(1.0, "法国 卫生部 中国 订购 总共 10亿 口罩"),
                RowFactory.create(1.0, "随着 美国 新冠病毒 大规模 爆发 求购 口罩 登天 还难"),
                RowFactory.create(1.0, "德国 新冠肺炎 累计 治愈 德国 新冠肺炎 累计 治愈"),
                RowFactory.create(1., "全球 新冠肺炎 累计 确诊"),
                RowFactory.create(1.0, "新冠肺炎 病毒 美国 国内 出现 大规模 爆发 迹象 美国 没有 对付 病毒 手段 美国 新冠肺炎 疫情 日益 恶化"),
                RowFactory.create(1.0, "法国 卫生部 中国 订购 总共 10亿 口罩"),
                RowFactory.create(1.0, "随着 美国 新冠病毒 大规模 爆发 求购 口罩 登天 还难"),
                RowFactory.create(1.0, "新冠肺炎 病毒 美国 国内 出现 大规模 爆发 迹象 美国 没有 对付 病毒 手段"),
                RowFactory.create(1.0, "全球 疫情 蔓延 各国 加码 防疫 力度 美国 新冠肺炎 疫情 日益 恶化 全美 报告 新冠肺炎 确诊 接近 10万 全球" +
                        " 首位" +
                        "美国 国务院 鼓励 各国 医生 赴美 帮助 抗疫 英国 首相 约翰逊 卫生大臣 汉考克 表示 新冠病毒 检测 呈阳性 意大利" +
                        " 27日 新增 死亡 达到 创纪录 969人 总统 马塔雷拉 签署 最新 防疫法 26日 正式 生效 西班牙 延长 国家 紧急 状态 15天" +
                        "伊朗 实施 社交疏远 计划 控制 疫情 德国 巨额 援助 计划 首批 资金 4月前 发放 日本 政府 全力 推进 新冠肺炎 药物 疫苗 研发")

        );

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                        new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
                }
        );

        Dataset<Row> sentenceData = spark.createDataFrame(dataCorpus, schema);
        // sentenceData.show(false);

        // 句子拆分为单词；
        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(sentenceData);  // 句子拆分为单词；

        /**
         * 模型配置
         */
        int size = 100; // //向量长度
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("words")
                .setOutputCol("rowFeatures")
                .setVocabSize(size) // .setMinDF(2)
                .fit(wordsData); // 数据拟合,返回模型

        // 本地语料库
        Dataset<Row> featurizedData = cvModel.transform(wordsData);
        featurizedData.cache();
        /**
         * 数据说明
         * +----------------------------------------------------+----------------------------------------+
         |rowFeatures                                         |words                                   |
         +----------------------------------------------------+----------------------------------------+
         |(15,[0,1,2,6,9,10,12],[2.0,2.0,1.0,1.0,1.0,1.0,1.0])|[你好, 我, 听过, spark, 而且, 我, 非常, 喜欢, spark]|
         一个14代表有15个单词;
         */
        // featurizedData.select("rowFeatures", "words").show(false);


        // 使用本地语料库进行idf模型训练
        IDF idf = new IDF().setInputCol("rowFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData); // 建立模型

        Dataset<Row> rescaledData = idfModel.transform(featurizedData); // 数据转化
        // rescaledData.select( "features").show(false);
        Dataset<Row> result = rescaledData.select("features"); // 得出计算结果
        //result.show(false);


//        System.out.println(resultJavaRDD.collect().get(0));

        // cvModel.vocabulary中存储有向量索引与原句子的词的对应关系
        String[] vocabulary = cvModel.vocabulary();
        Map<Integer, String> mapVocabulary = new HashMap<Integer, String>(vocabulary.length);
        int count = 0;
        for (String word : vocabulary
                ) {
            mapVocabulary.put(count++, word);
        }
        System.out.println(mapVocabulary);

        JavaRDD<String> tempJavaRDD = result.toJavaRDD().map(new Function<Row, String>() {
            public String call(Row row) throws Exception {
                // (15,[2,5,8,13],[0.28768207245178085,0.6931471805599453,0.6931471805599453,0.6931471805599453])
                String tmp = row.toString();
                int startIndex = tmp.indexOf(",");
                int endIndex = tmp.indexOf("])");
                // 字符截取 2,5,8,13],[0.28768207245178085,0.6931471805599453,0.6931471805599453,0.6931471805599453]
                String tmps = tmp.substring(startIndex + 2, endIndex);
                return tmps;
            }
        });

        List<String> list = tempJavaRDD.collect();
        for (String wordAndWeight : list
                ) {
            String[] temp = wordAndWeight.split("],\\[");
            String[] indexes = temp[0].split(",");
            String[] wieghts = temp[1].split(",");
            int count_ = 0;
            for (String index : indexes) {
                String word = mapVocabulary.get(Integer.parseInt(index));
                String weight = wieghts[count_];
                System.out.println(word + "=" + weight);
                count_++;
            }
            System.out.println("\n");

        }


        spark.close();
    }

    /**
     * 无法一一对应单词
     */
    public static void demo() {
        SparkConf conf = new SparkConf();
        conf.setAppName("TestSparkApp");
        conf.setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        List<Row> testData = Arrays.asList(
                RowFactory.create(0.0, "Hi I heard about Spark I wish Java could use case classes Logistic regression models are neat")

        );

        // 每一个句子代表一个文档
//        List<Row> dataCorpus = Arrays.asList(
//                RowFactory.create(0.0, "Hi I heard about Spark and I love spark"),
//                RowFactory.create(0.0, "I wish Java could use case classes"),
//                RowFactory.create(1.0,"Logistic regression models are neat")
//
//        );

        List<Row> dataCorpus = Arrays.asList(
                RowFactory.create(0.0, "你好 我 听过 spark 而且 我 非常 喜欢 spark"),
                RowFactory.create(0.0, "我 希望 Java 也 可以 使用 spark"),
                RowFactory.create(1.0, "逻辑回归 模型 非常 简洁")

        );

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                        new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
                }
        );

        Dataset<Row> sentenceData = spark.createDataFrame(dataCorpus, schema);
        // 句子拆分为单词；
        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(sentenceData);  // 句子拆分为单词；
        // wordsData.show(false);

        int size = 1000;
        HashingTF hashingTF = new HashingTF();
        hashingTF.setInputCol("words").setOutputCol("rowFeatures").setNumFeatures(size);
        // 本地语料库
        Dataset<Row> featurizedData = hashingTF.transform(wordsData);
        featurizedData.cache();
        // featurizedData.select("rowFeatures","words", "sentence").show(false);

        featurizedData.foreach(new ForeachFunction<Row>() {
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });

        System.out.println(featurizedData.first().size());
        featurizedData.first();


        // 使用本地语料库进行idf模型训练
        IDF idf = new IDF().setInputCol("rowFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);

        final Dataset<Row> rescaledData = idfModel.transform(featurizedData);
        // rescaledData.select("sentence", "features").show(false);
        // rescaledData.select("sentence","rowFeatures", "features").show(false);
        System.out.println();
        rescaledData.foreach(new ForeachFunction<Row>() {
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });

        spark.close();
    }

    public static void main1(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("TestSparkApp");
        conf.setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.close();
    }

    class ConvertToWords implements Function<String, Tuple2<String, Double>> {
        private Map<String, Integer> vocabulary;

        public Tuple2<String, Double> call(String v1) throws Exception {
            return null;
        }
    }
}

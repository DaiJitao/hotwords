package hot.words.service;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.algorithm.MaxHeap;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.summary.TextRankKeyword;
import hot.words.utils.HanlpUtil;
import hot.words.utils.TextCleaner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.util.*;

/**
 * Created by dell on 2020/3/26.
 */
public class TextRankKeyWords extends KeyWordsExtractor {


    public Map<String, Float> getKeyWords(JavaSparkContext sc, List<String> docs) {

        // 分词
        List<List<Term>> lists = HanlpUtil.nlpSegment(docs);
        List<Term> termList = new ArrayList<Term>(200);
        for (List<Term> term : lists
                ) {
            termList.addAll(term);
        }

        // 获取停用词词表
        List<String> stopwordsList = TextCleaner.stopWords;
        Broadcast<List<String>> stopwordsBroadcast = sc.broadcast(stopwordsList);

        JavaRDD<Term> termJavaRDD = sc.parallelize(termList);
        // 去除停用词
        JavaRDD<Term> removedwordsRDD = termJavaRDD.filter(new Function<Term, Boolean>() {
            @Override
            public Boolean call(Term term) throws Exception {
                List<String> stopWords = stopwordsBroadcast.getValue();
                // 停用词表
                if (stopWords.contains(term.word))
                    return false;
                return true;
            }
        });
        removedwordsRDD.persist(StorageLevel.MEMORY_AND_DISK());
        List<Term> removedwordsList = removedwordsRDD.collect();
        TextRankKeyword textRankKeyword = new TextRankKeyword();
        // 计算textRank
        Map<String, Float> map = textRankKeyword.getTermAndRank(removedwordsList);

        return map;
    }

    /**
     * textRank算法实现
     *
     * @param docs 多篇文档或一篇文档
     * @param size 关键词个数；-1 : all; >=0:提取指定值
     * @return
     * @throws IOException
     */
    public Map<String, Float> myKeyWords(List<String> docs, int size) throws IOException {
        // 分词
        List<List<Term>> lists = HanlpUtil.nlpSegment(docs);
        List<Term> termList = new ArrayList<Term>(200);
        for (List<Term> term : lists
                ) {
            termList.addAll(term);
        }
        int topN = size > 0 ? size : termList.size();

        TextRankKeyword textRankKeyword = new TextRankKeyword();
        // 计算textRank
        Map<String, Float> map = textRankKeyword.getTermAndRank(termList);
        // 提取topN
        Map<String, Float> resultMap = top(topN, map);

        return resultMap;
    }

    /**
     * hanlp自带的关键词抽取
     *
     * @param text
     * @return
     */
    public static List<String> extrKeywordByHanlp(String text) {
        List<String> result = new ArrayList<String>((int) (text.length() / 2));

        List<String> list = HanLP.extractKeyword(text, text.length());
        for (String word : list) {
            if (word.length() >= 2) {
                result.add(word);
            }
        }
        return result;
    }

    /**
     * 提取topN
     *
     * @param size:        关键词个数；<0: 取回所有; >=0:提取指定值
     * @param tfIDFMap:    tfidf计算的权重
     * @param textRankMap: textRank计算的权重
     * @return
     */
    public Map<String, Float> top(int topN, Map<String, Float> tfIDFMap, Map<String, Float> textRankMap) {
        Map<String, Float> resultMap = new LinkedHashMap<String, Float>();
        // 结果融合
        Map<String, Float> map = combineMap(tfIDFMap, textRankMap);
        // 取回结果个数
        int size = (topN < 0) ? map.size() : topN;
        // 建立最大heap
        for (Map.Entry<String, Float> entry : new MaxHeap<Map.Entry<String, Float>>(size, new Comparator<Map.Entry<String, Float>>() {

            public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        }).addAll(map.entrySet()).toList()) {
            resultMap.put(entry.getKey(), entry.getValue());
        }
        return resultMap;
    }

    /**
     * 提取topN
     *
     * @param size
     * @param map
     * @return
     */
    public Map<String, Float> top(int size, Map<String, Float> map) {
        Map<String, Float> result = new LinkedHashMap<String, Float>();
        for (Map.Entry<String, Float> entry : new MaxHeap<Map.Entry<String, Float>>(size, new Comparator<Map.Entry<String, Float>>() {
            public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        }).addAll(map.entrySet()).toList()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}

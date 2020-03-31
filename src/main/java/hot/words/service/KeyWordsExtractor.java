package hot.words.service;

import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer;
import com.hankcs.hanlp.seg.Dijkstra.DijkstraSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class KeyWordsExtractor implements Serializable {

    // 分词器
    protected static Segment nShortSegment = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true).enableAllNamedEntityRecognize(true);
    protected static Segment shortestSegment = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true).enableAllNamedEntityRecognize(true);
    protected static CRFLexicalAnalyzer crfSegment;
    // 维特比 静态方法，不用初始化
    protected static StandardTokenizer standardTokenizer;
    //
    protected static NLPTokenizer nlpTokenizer;

    static {
        try {
            crfSegment = new CRFLexicalAnalyzer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 实现文档分词
     *
     * @param docs: String:每一个元素是一个文档
     * @return
     */
    public List<List<Term>> cutWords(List<String> docs) {

        return null;

    }

    /**
     * map合并
     *
     * @param tfIDFMap
     * @param textRankMap
     * @return
     */
    protected Map<String, Float> combineMap(Map<String, Float> tfIDFMap, Map<String, Float> textRankMap) {
        Map<String, Float> resultMap = new HashMap<>(tfIDFMap.size() + textRankMap.size());
        // 遍历 tfIDFMap
        for (Map.Entry<String, Float> entry : tfIDFMap.entrySet()) {
            String words = entry.getKey();
            float tfidf_weight = entry.getValue();
            if (textRankMap.containsKey(words)) {
                float weight = (textRankMap.get(words) + tfidf_weight) / 2;
                resultMap.put(words, weight);
                textRankMap.remove(words); // 删除
            } else {
                resultMap.put(words, tfidf_weight);
            }
            resultMap.putAll(textRankMap);
        }
        return resultMap;
    }


}

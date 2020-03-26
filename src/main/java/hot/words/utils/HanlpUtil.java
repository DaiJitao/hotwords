package hot.words.utils;

import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer;
import com.hankcs.hanlp.seg.Dijkstra.DijkstraSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dell on 2020/3/26.
 */
public class HanlpUtil {
    private static final Segment nShortSegment = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true).enableAllNamedEntityRecognize(true);
    private static final Segment shortestSegment = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true).enableAllNamedEntityRecognize(true);

    /**
     * N-最短路径分词
     *
     * @param docs
     * @return
     */
    public static List<List<Term>> nShortSegment(List<String> docs) {
        List<List<Term>> result = new ArrayList<List<Term>>(10);
        for (String doc : docs) {
            List<Term> list = nShortSegment.seg(doc);
            result.add(list);
        }
        return result;
    }

    /**
     * 最短路径分词
     *
     * @param docs
     * @return
     */
    public static List<List<Term>> shortSegment(List<String> docs) {
        List<List<Term>> result = new ArrayList<List<Term>>(10);
        for (String doc : docs) {
            List<Term> list = shortestSegment.seg(doc);
            result.add(list);
        }
        return result;
    }

    /**
     * CRF分词
     *
     * @param docs
     * @return
     */
    public List<List<Term>> CRFSegment(List<String> docs) throws IOException {
        CRFLexicalAnalyzer analyzer = new CRFLexicalAnalyzer();

        List<List<Term>> result = new ArrayList<List<Term>>(10);

        for (String doc : docs) {
            List<Term> list = analyzer.seg(doc);
            result.add(list);
        }
        return result;
    }

    /**
     * 标准分词
     *
     * @param docs
     * @return
     */
    public static List<List<Term>> StandardSegment(List<String> docs) throws IOException {

        List<List<Term>> result = new ArrayList<List<Term>>(10);

        for (String doc : docs) {
            List<Term> list = StandardTokenizer.segment(doc);
            result.add(list);
        }
        return result;
    }

    /**
     * NLP分词
     *
     * @param docs
     * @return
     */
    public static List<List<Term>> nlpSegment(List<String> docs) throws IOException {

        List<List<Term>> result = new ArrayList<List<Term>>(10);

        for (String doc : docs) {
            List<Term> list = NLPTokenizer.segment(doc);
            result.add(list);
        }
        return result;
    }

    public static void main(String[] args) throws IOException {
        List<Term> termList = StandardTokenizer.segment("当红明星，商品和服务");
        List<String> docs = new ArrayList<String>();
        docs.add("我在上海林原科技有限公司兼职工作，" );
        docs.add("我经常在台川喜宴餐厅吃饭，");
        docs.add("偶尔去地中海影城看电影。");
        docs.add("当红明星，商品和服务");

        List<List<Term>> result = nlpSegment(docs);
        System.out.println("NLP:");
        for (List<Term> term : result) {
            System.out.println(term);
        }

        System.out.println("CRF:");
        HanlpUtil util = new HanlpUtil();
        result.clear();
        result = util.CRFSegment(docs);
        for (List<Term> terms : result ) {
            System.out.println(terms);
        }



    }
}

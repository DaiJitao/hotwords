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
     * 标准分词: 维特比分词器
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
     * NLP分词: 感知机分词器
     *
     * @param docs
     * @return
     */
    public static List<List<Term>> nlpSegment(List<String> docs) {

        List<List<Term>> result = new ArrayList<List<Term>>(docs.size());

        for (String doc : docs) {
            List<Term> list = NLPTokenizer.segment(doc);
            result.add(list);
        }
        return result;
    }

    public static List<String> loadExampleData() {
        String doc = "本报北京3月25日电（中青报·中青网记者 桂杰）人力资源和社会保障部副部长游钧在国新办今天举行的政策例行吹风会上说，" +
                "国有企业今明两年扩大招聘高校毕业生的规模，不得随意毁约，不得将在本单位实习的期限作为招聘入职的前提条件。" +

                "在拓宽高校毕业生就业渠道方面，人社部还提出，要扩大企业吸纳规模。对中小微企业招用毕业年度高校毕业生并签订1年以上劳动" +
                "合同的，给予一次性吸纳就业补贴。扩大基层就业规模，开发一批城乡社区等基层公共管理和社会服务岗位；扩大“三支一扶”计划" +
                "等基层服务项目招募规模；畅通民营企业专业技术职称评审渠道。"
               +
                "扩大2020年硕士研究生招生和专升本招生规模，扩大大学生应征入伍规模"
                +
                "扩大就业见习规模。对暂时中断见习的延长见习期限，对见习期未满与高校毕业生签订劳动合同的给予剩余期限见习补贴。" +

                "适当延迟录用接收。对延迟离校的毕业生，相应延长报到接收、档案转递、落户办理时限。对离校未就业毕业生提供两年的" +
                "户档保管便利，期间以应届生身份参加用人单位考试、录用，落实工作单位后，参照应届毕业生办理相关手续。";


        List<String> docs = new ArrayList<String>(10);
        docs.add(doc);
        docs.add(doc);
        docs.add("我是你的好朋友,你喜欢吗?");

        return docs;

    }

    public static void main2(String[] args) throws IOException {
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

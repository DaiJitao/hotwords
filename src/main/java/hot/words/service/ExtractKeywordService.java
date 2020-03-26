package hot.words.service;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.algorithm.MaxHeap;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.summary.TextRankKeyword;
import hot.words.utils.HanlpUtil;

import java.io.IOException;
import java.util.*;

/**
 * Created by dell on 2020/3/26.
 */
public class ExtractKeywordService {
    public static void main(String[] args) throws IOException {
        String doc = "本报北京3月25日电（中青报·中青网记者 桂杰）人力资源和社会保障部副部长游钧在国新办今天举行的政策例行吹风会上说，国有企业今明两年扩大招聘高校毕业生的规模，不得随意毁约，不得将在本单位实习的期限作为招聘入职的前提条件。\n" +
                "\n" +
                "　　在拓宽高校毕业生就业渠道方面，人社部还提出，要扩大企业吸纳规模。对中小微企业招用毕业年度高校毕业生并签订1年以上劳动合同的，给予一次性吸纳就业补贴。扩大基层就业规模，开发一批城乡社区等基层公共管理和社会服务岗位；扩大“三支一扶”计划等基层服务项目招募规模；畅通民营企业专业技术职称评审渠道。\n" +
                "\n" +
                "　　扩大2020年硕士研究生招生和专升本招生规模，扩大大学生应征入伍规模" +
                "\n" +
                "　　扩大就业见习规模。对暂时中断见习的延长见习期限，对见习期未满与高校毕业生签订劳动合同的给予剩余期限见习补贴。\n" +
                "\n" +
                "　　适当延迟录用接收。对延迟离校的毕业生，相应延长报到接收、档案转递、落户办理时限。对离校未就业毕业生提供两年的户档保管便利，期间以应届生身份参加用人单位考试、录用，落实工作单位后，参照应届毕业生办理相关手续。";


        List<String> list = new ArrayList<String>(10);
        list.add(doc);
        list.add(doc);

        ExtractKeywordService keywordService = new ExtractKeywordService();
        Map<String, Float> map = keywordService.myKeyWords(list,20);
        System.out.println(map);

    }

    /**
     * @param docs 多篇文档或一篇文档
     * @param size -1 : all; >=0:提取指定值
     * @return
     * @throws IOException
     */
    public Map<String, Float> myKeyWords(List<String> docs, int size) throws IOException {

        // 分词
        List<List<Term>> lists = HanlpUtil.nlpSegment(docs);
        List<Term> termList = new ArrayList<Term>(10);
        for (List<Term> term : lists
                ) {
            termList.addAll(term);
        }
        int topN = size > 0 ? size: termList.size();

        TextRankKeyword textRankKeyword = new TextRankKeyword();
        Map<String, Float> map = textRankKeyword.getTermAndRank(termList);
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
     * @param size
     * @param map
     * @return
     */
    private Map<String, Float> top(int size, Map<String, Float> map) {
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

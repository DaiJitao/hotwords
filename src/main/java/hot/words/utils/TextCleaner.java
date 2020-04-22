package hot.words.utils;

import com.google.common.base.Strings;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;
import org.apache.poi.util.StringUtil;
import sun.misc.Cleaner;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;


public class TextCleaner implements Serializable {
    public static List<String> stopWords ;
    static {
        stopWords = loadStopWords();
    }

    private static List<String> loadStopWords() {
        String file = Config.getValue("stopwords");
        String[] words = new String[0];
        try {
            words = FileUtil.loadData(file).split("\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Arrays.asList(words);
    }

    public static void main(String[] args) {
        String s = "  ";
        s = cleanText(s);
        System.out.println(isMached("公司)"));
    }

    // 匹配标点符号，匹配到返回true
    public static boolean isMached(String word){
        String patterns = "[|丨，{}【】。：；.“‘”…《》、！？——]{1,}";
        Pattern p = Pattern.compile(patterns);

        return p.matcher(word).find();
    }

    public static String cleanText(String doc) {
        String doc1 = doc.replaceAll("", ""); // 去除URL地址
        doc1 = doc1.replaceAll("'@.+@.+([\\n]|[\\t])", ""); //
        doc1 = doc1.replaceAll("#{1,}|//@|～|>{1,}|\\(\\)|[@·『』]", " ");
        doc1 = doc1.replaceAll("[。？\\?]{2,}", "？");
        doc1 = doc1.replaceAll("#.{1,6}#|展开全文|([a-zA-Z]|[0-9])", " ");
        // String r = "[/\uD83D\uDD25【】\uD83D\uDC47●■�→．・.\uD83D\uDCA9=\uD83D\uDC49（９\uD83D\uDC2D８７６５４３２１０／％［］\uD83D\uDE09×\\[\\]^_`{|}~(:з」∠)）\"①②③④]|(#|↓|(-  -)){1,}";
        // doc1 = doc1.replaceAll(r, " ");

        doc1 = doc1.replaceAll("[|丨✈\uD83C\uDF41\uD83D\uDE0B✨️❤\uD83D\uDD2A\uD83D\uDCB0\uD83D\uDC14\uD83C\uDF3A\uD83D\uDC2D &<…\uD83D\uDE4F]{1,}", " ");
        doc1 = doc1.replaceAll("—{3,}", "——");
        doc1 = doc1.replaceAll("\\s{1,}", " ").trim();
        doc1 = doc1.replaceAll("[^\u4e00-\u9fa5，{}【】（）。：；“‘”…《》、！？——]", ""); // 非中文
        return doc1;

    }

    public static String cleanSpecialCharactersText(String doc) {
        // 过滤特殊字符
        String doc1 = doc.replaceAll("[- +:\\/_,$%^*()+\\\"\\'<>]+|[+——~@#￥%&*【】{}《》“”‘’]+", "");
        String doc2 = doc1.replaceAll("\t", "").replaceAll("\n", "");
        return doc1;
    }

    // 去除停用词和标点符号
    public static String delStopWords(List<Term> termList) {
        List<String> words = new LinkedList<String>();
        for (Term term : termList) {
            // 单词长度大于1且不含有标点符号
            if (term.word.length() > 1 && !isMached(term.word))
                words.add(term.word);
        }
        words.removeAll(stopWords);
        return String.join(" ", words);
    }
}

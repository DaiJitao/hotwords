package hot.words.utils;

import com.google.common.base.Strings;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;
import org.apache.poi.util.StringUtil;
import sun.misc.Cleaner;

import java.io.File;
import java.util.*;


public class TextCleaner {
    public static List<String> stopWords = loadStopWords();

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


    public static String cleanSpecialCharactersText(String doc) {
        // 过滤特殊字符
        String doc1 = doc.replaceAll("[- +:\\/_,$%^*()+\\\"\\'<>]+|[+——~@#￥%&*【】{}《》“”‘’]+", "");
        String doc2 = doc1.replaceAll("\t", "").replaceAll("\n", "");
        return doc1;
    }

    // 去除停用词
    public static String delStopWords(List<Term> termList) {
        List<String> words = new LinkedList<String>();
        for (Term term : termList) {
            //
            if (term.word.length() > 1)
                words.add(term.word);
        }
        words.removeAll(stopWords);
        return String.join(" ", words);
    }
}

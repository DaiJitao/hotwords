import hot.words.utils.Config;

/**
 * Created by dell on 2020/3/26.
 */
public class HotWordsApp {
    public static void main(String[] args) {
        System.out.println(Config.getValue("stopwords"));

    }
}

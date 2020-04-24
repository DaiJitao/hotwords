package hot.words.utils;


import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dell on 2020/4/24.
 * 热词结果合并
 */
public class HotWordMerge {
    public static void main(String[] args) throws Exception {
        File[] files = FileUtil.listFilesByDir("");
        String jsonFile = "";
        List<String> list = FileUtil.loadJSONData(jsonFile);
        Map<String,Double> map = new HashMap<>();
        for(String doc : list){
            JSONObject.parseObject(doc);
        }
    }
}

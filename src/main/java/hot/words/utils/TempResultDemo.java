package hot.words.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TempResultDemo {

    public static void main(String[] args) throws Exception {
        String inFile = "E:\\data\\tempTask\\hotword\\tempresult\\myTemp3.txt";
        String outFile = "E:\\data\\tempTask\\hotword\\tempresult\\merged2.txt";
        List<String> list = FileUtil.loadJSONData(inFile);
        Map<String, Double> map = new HashMap<>(1000);
        for(String s:list){
            JSONObject jsonObject = JSONObject.parseObject(s);
            JSONArray array = jsonObject.getJSONArray("hotword");
            for(int i=0;i < array.size();i++ ){
                String word = array.getJSONObject(i).getString("word");
                double w = array.getJSONObject(i).getDouble("weight");
                if (map.containsKey(word)){
                    double t = (map.get(word) + w) / 2;
                    map.put(word,t);
                }else {
                    map.put(word,w);
                }
            }
        }
        FileUtil.save2Txt(outFile, map.toString());
    }
}

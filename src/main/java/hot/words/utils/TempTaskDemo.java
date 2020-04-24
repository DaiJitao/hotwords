package hot.words.utils;

import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dell on 2020/4/24.
 */
public class TempTaskDemo {


    public static void main(String[] args) throws Exception {
        //String srcDir = args[0];
        //String toDir = args[1];

        // String dstPath = srcDir ; // "F:\\data\\tempTask\\hotword\\"; // 数据目录
        String dstPath = "F:\\data\\tempTask\\hotword\\";
        File[] files = FileUtil.listFilesByDir(dstPath);
        String taskId = "hotworddjt_201804_201903_" + UUIDGenerator.getUUID();
        String outFile = "F:\\data\\tempTask\\year\\" + taskId + ".txt";
        // String outFile = toDir + "/" + taskId + ".txt";
        for (File file : files) {
            genFile(taskId, file.toString(), outFile);
            break;
        }
        System.out.println("写出文件：" + outFile);
    }


    private static void genFile(String taskId, String file, String outFile) throws Exception {
        List<String> docs = FileUtil.loadDataFromJSON(file);
        JSONObject resultObject = new JSONObject();
        JSONObject searchRule = new JSONObject();
        searchRule.put("eventWords", "");
        searchRule.put("regionWords", "");
        searchRule.put("organizationWords", "");
        searchRule.put("personWords", "");
        List<String> list = new ArrayList<>(docs.size());
        int i = 0;
        for (String cotent : docs) {
            String assetId = UUIDGenerator.getUUID() + (i++);
            resultObject.put("taskType", "2");
            resultObject.put("searchRule", searchRule);
            resultObject.put("infoType", "");
            resultObject.put("kafkaTopicName", "HotwordTopic");
            resultObject.put("assetId", assetId);
            resultObject.put("title", "");
            resultObject.put("taskId", taskId);
            resultObject.put("content", cotent);
            list.add(resultObject.toJSONString());
        }
        FileUtil.save2Txt(outFile, String.join("\n", list), true);
    }
}



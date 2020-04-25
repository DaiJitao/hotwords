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
        String srcPath = "E:\\data\\tempTask\\hotword\\data\\";
        File[] files = FileUtil.listFilesByDir(srcPath);

        // String outFile = toDir + "/" + taskId + ".txt";
        int count = 1;
        for (File file : files) {
            String taskId = "hotworddjt2_"+ count + "_201804_201903_" + UUIDGenerator.getUUID();
            String outFile = "E:\\data\\tempTask\\hotword\\out_1month\\" + taskId + ".txt";
            System.out.println("文件 taskId="+taskId);
            genFile(taskId, file.toString(), outFile);

            System.out.println("输出文件：" + outFile);
            count++;
        }

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
        FileUtil.save2Txt(outFile, String.join("\n", list));
    }
}



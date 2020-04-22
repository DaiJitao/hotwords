package hot.words.utils;

import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by dell on 2020/4/3.
 */
public class PerformanceTest {


    public static void main(String[] args) throws Exception {
        String dstPath = "F:\\data\\hotwordner\\";
        File[] rmfiles = FileUtil.listFilesByDir(dstPath);
        for (File file : rmfiles) {
            FileUtil.deleteFile(file);
        }

        int pSize = 100; // 并发量，线程数,文件数
        int dataSize = 1000; // 每个文件的数据量:条数
        for (int i = 0; i < pSize; i++) {
            genFile(dataSize, pSize);
        }
        Thread.sleep(500);

        File[] files = FileUtil.listFilesByDir(dstPath);
        List<Thread> taskList = new ArrayList<>(files.length);
        for (File file : files) {
            int s = file.toString().lastIndexOf("\\");
            int e = file.toString().indexOf(".");
            String taskId = file.toString().substring(s + 1, e);

            Runnable task = new SendTask(file.toString(), taskId);
            Thread thread = new Thread(task);
            taskList.add(thread);
        }
        for (Thread task : taskList) {
            task.start();
        }
    }

    /**
     * @param size 数据条数
     * @param threadNum 线程数
     * @throws Exception
     */
    private static void genFile(int size, int threadNum) throws Exception {
        String path = "F:\\data\\hotwordner_test2_f7ea8b_20200306175137_20200306175137.txt";


        String dstPath = "F:\\data\\hotwordner\\";
        String data = FileUtil.loadData(path).trim();
        JSONObject jsonObject = JSONObject.parseObject(data);
        String taskId = "hotwordner_test_" + threadNum + "_" + size + "_" + UUIDGenerator.getUUID();
        String file = dstPath + taskId + ".txt";
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String assetId = UUIDGenerator.getUUID() + i;
            jsonObject.put("assetId", assetId);
            jsonObject.put("taskId", taskId);
            jsonObject.put("taskType", "2");
            list.add(jsonObject.toJSONString());
        }
        FileUtil.save2Txt(file, String.join("\n", list));
    }

}

class SendTask implements Runnable {
    HdfsClientUtil clientUtil = HdfsClientUtil.getInstance();

    private String localFile;
    private String newName;

    public SendTask(String localFile, String newName) {
        this.localFile = localFile;
        this.newName = newName;
    }

    @Override
    public void run() {

        try {
            String name = newName + ".txt";
            String time = DateUtil.getCurrentTime2("8");
            clientUtil.uploadToMonitor(localFile, name);
            FileUtil.save2Txt("F:/data/send_task.txt", newName + ":" + time + "\n", true);
            System.out.println(newName + " 发送时间:" + time);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

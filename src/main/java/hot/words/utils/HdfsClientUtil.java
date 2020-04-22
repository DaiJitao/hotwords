package hot.words.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * hadoop文件系统操作
 * 注意：configuration设置需要根据具体部署环境修改
 *
 * @author Administrator
 */

public class HdfsClientUtil {
    private final static Logger logger = LoggerFactory.getLogger(HdfsClientUtil.class);
    private static HdfsClientUtil instance = new HdfsClientUtil();
    private static Configuration conf = new Configuration();
    public static FileSystem fs;

    static {
        //添加配置
        conf.set("fs.defaultFS", Config.getValue("fs.defaultFS"));
        conf.set("dfs.nameservices", Config.getValue("dfs.nameservices"));
        conf.set("dfs.ha.namenodes.amend", Config.getValue("dfs.ha.namenodes.amend"));
        conf.set("dfs.namenode.rpc-address.amend.carbigdata4", Config.getValue("dfs.namenode.rpc-address.amend.carbigdata4"));
        conf.set("dfs.namenode.rpc-address.amend.carbigdata5", Config.getValue("dfs.namenode.rpc-address.amend.carbigdata5"));
        conf.set("dfs.client.failover.proxy.provider.amend", Config.getValue("dfs.client.failover.proxy.provider.amend"));
        try {
            fs = FileSystem.get(conf);
            logger.info("创建FileSystem成功，" + conf.toString());
        } catch (Exception e) {
            logger.info("创建FileSystem失败," + conf.toString());
        }
        init();
    }


    private static void init() {
        String tempDir = Config.getValue("tempDir");
        String taskDir = Config.getValue("taskDir");
        Path tempPath = new Path(tempDir);
        Path taskPath = new Path(taskDir);
        try {
            if (!fs.exists(tempPath)) {
                fs.mkdirs(tempPath);
            } else if (!fs.exists(taskPath)) {
                fs.mkdirs(taskPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("初始化目录" + taskDir + " " + tempDir + " 失败！");
            System.exit(1);
        }
    }

    private HdfsClientUtil() {
    }

    public static HdfsClientUtil getInstance() {
        return instance;
    }

    //创建新文件
    public void createFile(String dst, byte[] contents) throws IOException {
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        System.out.println("文件：" + dstPath + " 创建成功！");
    }


    //上传本地文件
    private void uploadFile(String src, String dst) throws IOException {
        Path srcPath = new Path(src); //本地上传文件路径
        Path dstPath = new Path(dst); //hdfs目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false, srcPath, dstPath);
        //打印文件路径
        FileStatus[] fileStatus = fs.listStatus(dstPath);

    }

    //文件重命名
    public boolean rename(String oldName, String newName) throws Exception {
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean bResult = false;
        try {
            bResult = fs.rename(oldPath, newPath);
            return bResult;
        } catch (IOException e) {
            throw new Exception("文件重命名失败");
        }
    }

    //删除文件
    public boolean deleteFile(String file) throws IOException {
        Path filePath = new Path(file);
        boolean isExist = fs.exists(filePath);
        if (isExist) {
            // boolean isDelete = fileSystem.deleteOnExit(filePath);
            boolean isDelete = fs.delete(filePath, true);
            return isDelete;
        } else {
            // 文件已不存在
            return true;
        }
    }

    //删除文件
    public boolean deleteFile(Path file) throws IOException {
        boolean isExist = fs.exists(file);
        if (isExist) {
            // boolean isDelete = fileSystem.deleteOnExit(filePath);
            boolean isDelete = fs.delete(file, true);
            return isDelete;
        } else {
            // 文件已不存在
            return true;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        HdfsClientUtil util = HdfsClientUtil.getInstance();
//        List<Path> paths = util.listDirectoryFromHdfs("/hotwordNer/taskDir");
//        for (Path file : paths) {
//            if (file.toString().contains("20200416"))
//                util.deleteFile(file);
//
//        }
        // util.deleteFile()
        String srcFile = "G:\\新华网项目\\实体词热词\\寻找最好批次\\6.txt";

        for (int i = 0; i < 10; i++) {
            Thread.sleep(10000);
            String name = "test_" + UUIDGenerator.getUUID() + ".txt";
            util.uploadToMonitor(srcFile, name);
            System.out.println(name + "已上传\n");
        }

    }


    /**
     * @Description: 创建目录，如果父目录不存在则级联创建
     * @Param: [strDirPath] 待创建的目录，比如/test
     * @return: boolean
     * @Author: Mr.Young
     * @Date: 2018-11-21
     */
    public boolean mkdirs(String dir) {
        Path objPath = new Path(dir);
        boolean bResult = false;
        try {
            bResult = fs.mkdirs(objPath);
            if (bResult) {
                logger.info("目录 " + objPath + " 创建成功!");
            } else {
                logger.error("目录 " + objPath + " 创建失败！");
            }
        } catch (IOException ioe) {
            logger.error(ioe.getStackTrace().toString());
            ioe.printStackTrace();
        }
        return bResult;
    }

    //读取文件的内容
    public void readFile(String filePath) throws IOException {
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
        } finally {
            IOUtils.closeStream(in);
        }
    }


    /**
     * 遍历指定目录(direPath)下的所有文件
     */
    public List<Path> listDirectoryFromHdfs(String direPath) {
        List<Path> list = new ArrayList<>(100);
        try {
            FileStatus[] filelist = fs.listStatus(new Path(direPath));

            for (int i = 0; i < filelist.length; i++) {

                FileStatus fileStatus = filelist[i];
                // System.out.println(fileStatus.getPath().getName());
                // System.out.println(fileStatus.getPath());
                list.add(fileStatus.getPath());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * @Description: 将hdfs文件拷贝到本地
     * @Param: [src hdfs文件路径, dst 本地文件路径]
     * @return: boolean 成功返回true，失败返回false
     * @Author: Mr.Young
     * @Date: 2018-11-21
     */
    public boolean downloadFile(String src, String dst) {
        Path file = new Path(src);
        Path dstPath = new Path(dst);
        boolean bResult = false;
        System.out.println("下载文件：" + file);
        try {
            fs.copyToLocalFile(file, dstPath);
            bResult = true;
        } catch (IOException ioe) {
            logger.error("文件下载失败！" + ioe.getStackTrace());
        }
        return bResult;
    }

    /**
     * @Description: 上传文件到HDFS目录
     * @Param: [localFile 本地文件 /home/file.txt, fileName 本地文件名 file.txt]
     * @return: boolean 成功返回true, 失败返回false
     * @Author: Mr.Young
     * @Date: 2018-10-22
     */
    public boolean uploadToMonitor(String localFile, String fileName) {
        String dfsTempDir = Config.getValue("tempDir") + fileName;
        String dfsTaskDir = Config.getValue("taskDir") + fileName;
        boolean isOK = false;
        try {
            // 将文件上传到dfsTempDir临时目录下
            uploadFile(localFile, dfsTempDir);
            logger.info("{}文件上传到dfs临时目录{}成功", localFile, dfsTempDir);
            System.out.println(localFile + "文件上传到dfs目录成功" + dfsTempDir);
        } catch (IOException e) {
            logger.error("{}文件上传到dfs临时目录{}失败，exception:{}", localFile, dfsTempDir, e.toString());
            e.printStackTrace();
        }

        try {
            // 将dfsTempDir目录的文件移到dfsMonitorDir目录下
            boolean bResunt = rename(dfsTempDir, dfsTaskDir);
            if (bResunt) {
                logger.info("{}文件移动到dfsTask目录{}成功", dfsTempDir, dfsTaskDir);
                System.out.println(dfsTempDir + " 文件移动到dfsTask目录成功" + dfsTaskDir);
                isOK = true;
            } else {
                logger.error("{}文件移动到dfsTask目录{}失败", dfsTempDir, dfsTaskDir);
                System.out.println(dfsTempDir + " 文件移动到dfsTask目录成功" + dfsTaskDir);
            }
        } catch (Exception e) {
            logger.error("{}文件移动到dfsTask目录{}失败", dfsTempDir, dfsTaskDir);
            System.out.println(dfsTempDir + "文件移动到dfsTask目录失败" + dfsTaskDir);
            e.printStackTrace();
        }
        return isOK;
    }
}

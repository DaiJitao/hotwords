package hot.words.utils;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by dell on 2019/7/22.
 */
public class FileUtil {

    public static boolean mkdir(String dir) {
        boolean isCreated = false;
        File file = new File(dir);
        if (file.exists()) {
            isCreated = true;
        } else if (!file.exists()) {
            isCreated = file.mkdirs();
        }
        return isCreated;
    }


    public static File[] listFilesByDir(String dir) {
        File pathFile = new File(dir);
        if (pathFile.isDirectory()) {
            File[] files = pathFile.listFiles();
            return files;
        } else {
            return new File[]{pathFile};
        }
    }

    /**
     * 按照行读取文件
     *
     * @param file
     * @return
     */
    public static String loadData(File file) throws Exception {
        FileInputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        StringBuilder stringBuilder = new StringBuilder();
        try {
            String charSet = "UTF-8";
            fileInputStream = new FileInputStream(file);
            inputStreamReader = new InputStreamReader(fileInputStream, charSet);
            bufferedReader = new BufferedReader(inputStreamReader);
            String line = bufferedReader.readLine();
            while (line != null) {
                stringBuilder.append(line).append("\n");
                line = bufferedReader.readLine();
            }
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("没有找到" + file);
        } catch (UnsupportedEncodingException e) {
            throw new FileNotFoundException("不支持编码" + file);
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            try {
                if (bufferedReader != null)
                    bufferedReader.close();
                if (null != inputStreamReader)
                    inputStreamReader.close();
                if (null != fileInputStream)
                    fileInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return stringBuilder.toString();
    }

    public static void save2Txt(String fileName, String content) throws Exception {
        //1 创建文件
        File file = new File(fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new Exception(e);
            }
        }
        //2 write
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false), Charset.forName("UTF-8")));
            writer.write(content);
        } catch (IOException e) {
            throw new Exception(e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    throw new Exception(e);
                }
            }
        }
    }


}
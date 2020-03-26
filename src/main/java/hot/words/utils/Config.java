package hot.words.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * @program: BigMANService
 * @description: hadoop config的相关配置
 * @author: Mr.Young
 * @create: 2018-10-18 17:00
 **/

public class Config implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(Config.class);

    static Properties prop = new Properties();
    static {
        InputStream in = Config.class.getClassLoader().getResourceAsStream("hotwords.properties");
        try {
            prop.load(in);
            logger.info("读取配置文件hotwords.properties成功！");
        } catch (Exception e) {
            logger.error("读取配置文件hotwords.properties失败！", e);
        }
    }

    public static String getValue(String key){
        return prop.getProperty(key).trim();
    }
}

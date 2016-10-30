package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;


public class PropertyUtil {
    private static Logger logger = Logger.getLogger(PropertyUtil.class);
    private static String propFilePath = getCurrentJarPath() + "prop.properties";
    public static Properties prop = null;

    /**
     * 读取并加载配置文件
     * 
     * @param ConfigFileName
     *            配置文件地址
     * @return
     */
    public static void loadConfig() {
        try {
            InputStream inputStream = new FileInputStream(new File(propFilePath));
            prop = new Properties();
            prop.load(inputStream);
            // 获取写入列
        } catch (Exception ex) {
            logger.error(ex);
        }
    }

    /**
     * 获取当前目录地址
     * 
     * @return
     */

    public static String getCurrentJarPath() {
        String c_path = PropertyUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String os_name = System.getProperty("os.name").toLowerCase();
        c_path = os_name.startsWith("win") ? c_path.substring(1, c_path.lastIndexOf("/") + 1) : c_path.substring(0, c_path.lastIndexOf("/") + 1);
        return c_path;
    }

    public static void main(String[] args) {
//        String s=getCurrentJarPath();
//        System.out.println("s="+s);
        loadConfig();
//        Enumeration<Object> eks=prop.keys();
//        while (eks.hasMoreElements()) {
//            Object object = (Object) eks.nextElement();
//            System.out.println(object+"="+prop.get(object));
//        }
        String regEx=prop.getProperty("regEx");
        String t="startTag\\[([\\s\\S]*?)\\]endTag";
        System.out.println(t);
        System.out.println(regEx);
    }
}

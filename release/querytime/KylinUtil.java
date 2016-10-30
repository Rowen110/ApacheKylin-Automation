package com.njws.getmaintabletime;


	import java.io.BufferedReader;
	import java.io.File;
	import java.io.FileInputStream;
	import java.io.FileNotFoundException;
	import java.io.IOException;
	import java.io.InputStreamReader;
	import java.util.ArrayList;
	import java.util.HashMap;
	import java.util.HashSet;
	import java.util.List;
	import java.util.Map;
	import java.util.Set;
	import java.util.regex.Matcher;
	import java.util.regex.Pattern;
	import org.slf4j.Logger;
	import org.slf4j.LoggerFactory;
	import com.htsc.morphling.applierapi.ApplierContent;
	import com.jcraft.jsch.JSchException;
	
	public class KeylinUtil {

	    private static final Logger logger = LoggerFactory.getLogger(KeylinUtil.class);
	    class MainTableInfo{
	        String mainTableName;
	        String cubeName;
	        String key;
	        String timeColumn;
	        String projectName;
	        String sql;
	        public MainTableInfo(String mainTableName,String cubeName, String key, String timeColumn, String projectName, String sql) {
	            super();
	            this.mainTableName=mainTableName;
	            this.cubeName = cubeName;
	            this.key = key;
	            this.timeColumn = timeColumn;
	            this.projectName = projectName;
	            this.sql = sql;
	        }
	    }
	    static KylinApi api = new KylinApi();
	    Map<String,Map<String,MainTableInfo>> initMap;
	    /**
	     * 方法说明 方法名：matchDateString 参数说明：调用的kylin API返回的buf值
	     * 功能说明：通过正则表达式匹配buf中yyyy-mm-dd格式的内容，转存到数组，将修改后的"yyyy-mm-dd 主表名“格式返回
	     */
	    public List<String> matchDateString(StringBuffer ans,String mainTable) {
	        Pattern p = Pattern.compile("(\\d{1,4}[-|\\/|年|\\.]\\d{1,2}[-|\\/|月|\\.]\\d{1,2}([日|号])?)",
	                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
	        Matcher matcher = p.matcher(ans);
	        List<String> matcheList = new ArrayList<String>();
	        while (matcher.find()) {
	            matcheList.add(matcher.group()+" "+mainTable);
	        }
	        return matcheList;
	    }

	    /**
	     * 方法说明 方法名：readfile 参数说明：配置文件 功能说明： ①读取配置文件 ②将该文件的第一列与第二列存入HashMap
	     * (table),table的key为建模的表名，value为对应的主表名；
	     * ③将该文件的第一列与第七列存入HashMap（sql），sql的key为建模的表名，value为查询分表关主表对应时间字段的SQL语句。
	     */

	    public void readfile(String file) {}

	    /**
	     * 方法说明 方法名：testCubeQuery 参数说明：分表主键（lookupid) 功能说明：调用kylin API查询分表关联主表的时间字段值
	     */
	    public StringBuffer testCubeQuery(String sql, String lookupId, String projectName) {
	        StringBuffer timeBuf = new StringBuffer();
	         timeBuf=api.getTableTime(sql,lookupId,projectName);
	        return timeBuf;
	    }

	    public boolean judge(String lookuptable, String maintable) {
//	        if (!lookuptable.equals("ws_oa_newoauat_t_department")
//	                && !lookuptable.equals("ws_oa_newoauat_t_userinfo")) {
//	            return true;
//	        }
//	        return false;
	        return true;
	    }

	    /**
	     * 方法说明 方法名：f 参数说明：str1：要建模的表，lookupid：分表主键，fact：原定义为主表表名，现不用但保留。
	     * 方法说明：如果要建模的表是分表，则通过调用kylin API获得该分表关联主表的时间字段，返回“yyyy-mm-dd 主表名”的set集合
	     */
	    public Set<String> findInfos(String lookuptable, String lookupid, Set<String> fact) throws JSchException, IOException {
	        //logger.info("enter KeylinUtil findInfos");
	        //logger.info("lookuptable=" + lookuptable);
	        //logger.info("lookupid=" + lookupid);
	        StringBuffer bb = new StringBuffer();
	        Set<String> set = new HashSet<String>();
	        Map<String,MainTableInfo> mainTableMap=initMap.get(lookuptable);
	        if (mainTableMap!=null) {
	            Set<String> maintableSet=mainTableMap.keySet();
	            for (String maintable:maintableSet) {
	                if (judge(lookuptable, maintable)) {
	                    bb = testCubeQuery(mainTableMap.get(maintable).sql, lookupid, mainTableMap.get(maintable).projectName);
	                    List<String> infoList = matchDateString(bb, maintable);
	                    for (int i = 0; i < infoList.size(); i++) {
	                        set.add(infoList.get(i));
	                    }
	                }
	            }
	            //logger.info("this set="+set);
	        }else{
	            logger.error("this lookuptable:"+lookuptable+" not find any mainTable");
	        }
	        return set;
	    }
	    
	    public KeylinUtil(){
	        initMap=new HashMap<String, Map<String,MainTableInfo>>();
	        // HashMap（sql），sql的key为建模的表名，value为查询分表关主表对应时间字段的SQL语句。
	        // 设置当前文件名字
	        String filename = ApplierContent.getInstance().getConfig().getString("tableInfoFilePath");
	        // 初始化读取文件操作
	        File file1 = new File(filename);
	        FileInputStream fis = null;
	        InputStreamReader isr = null;
	        BufferedReader br = null;
	        String line = null;
	        // 遍历读取文件
	        try {
	            fis = new FileInputStream(filename);
	            isr = new InputStreamReader(fis);
	            br = new BufferedReader(isr);
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        }
	        try {
	            int lineIndex=0;
	            while ((line = br.readLine()) != null) {
	                lineIndex++;
	                String[] infos = line.split("\t");
	                if (infos.length==7) {
	                    String lookuptable = infos[0];
	                    String cubeName=infos[1];
	                    String mainTableName=infos[2];
	                    String timeColumn=infos[3];
	                    String key=infos[4];
	                    String projectName=infos[5];
	                    String sql=infos[6];
	                    MainTableInfo mainTableInfo =new MainTableInfo(mainTableName,cubeName, key, timeColumn, projectName, sql);
	                    if (initMap.get(lookuptable)==null) {
	                        initMap.put(lookuptable, new HashMap<String,MainTableInfo>());
	                    }
	                    initMap.get(lookuptable).put(mainTableName, mainTableInfo);
	                }else{
	                    logger.error("line "+lineIndex+":this line has some format problems");
	                }
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            // 关闭文件操作
	            try {
	                br.close();
	                isr.close();
	                fis.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	    }
	}



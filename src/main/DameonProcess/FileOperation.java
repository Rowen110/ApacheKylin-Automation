package com.njws.checkjoberror;

import java.io.*;
import java.util.*;
import org.apache.log4j.Logger;

/**类的说明
 * 类名：FileOperation
 * 作者：柏晨浩
 * 日期：2016年9月18日
 * 类的功能：完成对配置文件和kafka那边处理生成的文件的读取写入功能
 */
public class FileOperation {
	/**
	 * 存放配置文件中cube的名称
	 */
		String cubeName;
	/**
	 * 存放配置文件中项目名称
	 */
		String projectName;
	/**
	 * key值为cube名称，value值为cube对应的项目名称
	 */
		Map<String,String> cubeProject = new HashMap<String,String>();
	/**
	 * 记录日志信息
	 */
		private static Logger logger = Logger.getLogger(FileOperation.class);  
		
		
/**
 * 	追加写入：将调用接口请求到的job状态，追加写入至日志
 * @param jobLogFile
 * @param jobStatus
 */
public void  writeToLogFile(String jobLogFile, String jobStatus) {  
		logger.info("将job状态写入job日志文件"+"joblogFile:	"+jobLogFile+"	"+"jobStatus: "+jobStatus);
	    BufferedWriter out = null;     
	    try {     
	        out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(jobLogFile)));     
	        out.write(jobStatus);     
	    } catch (Exception e) {     
	        e.printStackTrace();     
	    } finally {     
	        try {     
	            if(out != null){  
	                out.close();     
	            }  
	        } catch (IOException e) {     
	            e.printStackTrace();     
	        }     
	    }     
	}  

/**
 * 读取配置文件
 * @param readConfigFile
 */
public void readConfigFile(String configFile){
			//设置当前文件名字
			String filename=configFile;
			 //初始化读取文件操作
			FileInputStream fis = null;
			 InputStreamReader isr = null;
			 BufferedReader br = null;
			 String line = null;
			 //遍历读取文件
			 try{
				     fis = new FileInputStream(filename);
				     isr = new InputStreamReader(fis);
				     br = new BufferedReader(isr);
				   }
			 catch (FileNotFoundException e) {
				     e.printStackTrace();
				   }
			 try {
				      while ((line = br.readLine()) != null) {
				      //这里自己解析内容  line.spilt("\t"),  1是建模的表对应的cube名，2是建模的表关联的主表名
				      String[] infos=line.split("\t");
				      cubeName=infos[1];
				      projectName=infos[5];
				      cubeProject.put(cubeName,projectName);
		 			}
				 } 
			 catch (IOException e){
				    e.printStackTrace();
			}
			 finally{
				    //关闭文件操作
				    try {
				        	br.close();
				        	isr.close();
				        	fis.close();
				        }
				     catch (IOException e){
				    	 e.printStackTrace();
				        }
				   }
		 }
} 
	


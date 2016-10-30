package com.njws.oadataimport;

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
	 * 存放配置文件中主表名称
	 */
		String mainTable;
	/**
	 * 存放配置文件中项目名称
	 */
		String projectName;
	/**
	 * 存放待导入数据的时间列
	 */
		String time;
	/**
	 * 将待导入数据以及等待数据的时间列在初始化时存放至timeSet集合中
	 */
		Set<String> timeSet=new HashSet<String>();
	/**
	 * key值为cube名，value值为项目名
	 */
		Map<String,String> cubeProject= new HashMap<String,String>();
	/**
	 * key值为主表名，value值为cube名
	 */
		Map<String,String> tableCube=new HashMap<String, String>();
	/**
	 * key值为主表名，value值为项目名
	 */
		Map<String,String> tableProject = new HashMap<String,String>();
	/**
	 * key值为时间列，value值为主表名
	 */
		Map<String,String> timeTable=new HashMap<String, String>();		//存储文件中的时间与表
	/**
	 * 记录日志操作
	 */
		private static Logger logger = Logger.getLogger(FileOperation.class);  
	/**
	 * key值为时间列，value值为以主表为key，以MainTableInfo为value的map
	 */
		Map<String,Map<String,MainTableInfo>>  initMap = new HashMap<String,Map<String,MainTableInfo>>();
		
/**
 * 存放主表信息
 * @author 柏晨浩
 */
	class MainTableInfo{
			String table;
	        public MainTableInfo(String table) {
	            super();
	            this.table = table;
	        }
	    }
		
/**
 * 	覆盖写入：将WaitSet集合中的内容持久化到磁盘，以防程序崩溃致使数据丢失
 * @param setFile
 * @param waitSet
 */
public void writeToSetFile(String setFile,String waitString) { 
	 logger.info("将waitSet集合写入setFile文件 "+"setFile:	"+setFile+" "+"waitSet："+waitString);
	 try {
		    File file = new File(setFile);
			if (!file.exists()) {
				 file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
		    BufferedWriter bw = new BufferedWriter(fw);
		    bw.write(waitString);
		    bw.close();
	 	} catch (IOException e) {
		   e.printStackTrace();
		}
	}

/**
 * 读取配置文件
 * @param readConfigFile
 */
public void readConfigFile(String configFile){
//			logger.info("读取配置文件: "+configFile);
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
				      mainTable=infos[2];
				      projectName=infos[5];
				       tableCube.put(mainTable, cubeName);    	//将建模的表对应的主表名以及cube名放进HashMap中，其中Key值为主表名，value值为cube名。 
				       tableProject.put(mainTable, projectName);//将建模的表对应的时间以及项目名称放进HashMap中，其中Key值为时间，value值为项目名 。
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

/**
 * 读取setFile文件
 * @param setFile
 */
public	void readSetFile(String setFile){
	   File file = new File(setFile);
		if (!file.exists()) {
				try {
					file.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			    //设置当前文件名字
			    String filename=setFile;
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
		 		            String[] infos = line.split(" ");
		 		            if (infos.length==2) {
		 		            	String time = infos[0];
		 		            	String mainTableName = infos[1];
		 		            	timeSet.add(time);
		 		                MainTableInfo mainTableInfo =new MainTableInfo(mainTableName);
		 		                if (initMap.get(time)==null) {
		 		                    initMap.put(time, new HashMap<String,MainTableInfo>());
		 		                }
		 		                initMap.get(time).put(mainTableName, mainTableInfo);
		 		            	}
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
			        					file.delete();
			        				}
			        			catch (IOException e){
			        					e.printStackTrace();
			        		}
			        	}
					}
						
/**
 * 	读取处理kafka数据后生成的文件，读取完毕后删除
 * @param readdataFile
 */
 public	void readDataFile(String dataFoldPath) {
	 		String refreshFolderPath=dataFoldPath;
	 		File file1 = new File(refreshFolderPath);
	 		//获得当前文件夹所有的文件
	 		String [] fileNames = file1.list();
	 		FileInputStream fis = null;
	 		InputStreamReader isr = null;
	 		BufferedReader br = null;
	 		String line = null;
	 		List<String> thisFileNames=new ArrayList<String>();
	 		Arrays.sort(fileNames);
	 		for (int i = 0; i < fileNames.length; i++) {
	 			if (!fileNames[i].contains("tmp")) {
	 				thisFileNames.add(fileNames[i]);
				}
			}	 		
	 		//遍历读取文件
	 		for (String fileName:thisFileNames){
	 			try {
	 					fis = new FileInputStream(refreshFolderPath+fileName);
	 					isr = new InputStreamReader(fis);
	 					br = new BufferedReader(isr);
	 				}
	 			catch (FileNotFoundException e) {
	 					e.printStackTrace();
	 				}
	 			try {
	 		        while ((line = br.readLine()) != null) {
	 		            String[] infos = line.split(" ");
	 		            if (infos.length==2) {
	 		            	String time = infos[0];
	 		            	String mainTableName = infos[1];
	 		            	timeSet.add(time);
	 		                MainTableInfo mainTableInfo =new MainTableInfo(mainTableName);
	 		                if (initMap.get(time)==null) {
	 		                    initMap.put(time, new HashMap<String,MainTableInfo>());
	 		                }
	 		                initMap.get(time).put(mainTableName, mainTableInfo);
	 		            }
	 				}
	 			}
	 			catch (IOException e){
	 						e.printStackTrace();
	 					}
	 			finally {
	 					try{
	 							br.close();
	 							isr.close();
	 							fis.close();
	 						}
	 					catch (IOException e){
	 							e.printStackTrace();
	 						}
	 				}
	 		}	
	     //删除这批读取的文件
	     for (String fileName:thisFileNames)
	 		{
	         File sonFile=new File(refreshFolderPath+fileName);
	         sonFile.delete();
	     	}
	 	}
} 
	


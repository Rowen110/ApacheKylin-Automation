package com.njws.checkjoberror;

import util.PropertyUtil;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.log4j.Logger;
import org.json.*;

/**
 * 类的说明 类名：JsonCompilation 作者：柏晨浩 时间：2016年9月18日 类的功能：调用kylin接口，对buf中的返回json串进行解析
 */
public class JsonCompilation {

	/**
	 * 存放getCubeList返回buf的json数组的大小
	 */
	int cSize;
	/**
	 * 存放compileJobJson返回bu的json数组的大小
	 */
	int jSize;
	/**
	 * 存放当前系统时间
	 */
	String systemTime;
	/**
	 * 存放请求kylin获取的jobid
	 */
	String[] jobUuid;
	/**
	 * 通过getCubeLists请求kylin返回的buf
	 */
	StringBuffer cubeBuf = new StringBuffer();
	/**
	 * 通过getjobId请求kylin返回的buf
	 */
	StringBuffer jobBuf = new StringBuffer();
	/**
	 * 存放jobLog
	 */
	StringBuffer jobLogBuf = new StringBuffer();
	/**
	 * 存放jobErrorLog
	 */
	StringBuffer jobErrorBuf = new StringBuffer();
	/**
	 * 存放当前所有的cube名
	 */
	Set<String> cubeSet = new HashSet<String>();
	/**
	 * 实例化文件读写类
	 */
	FileOperation rw = new FileOperation();
	/**
	 * 实例化KylinApi类
	 */
	KylinApi api = new KylinApi();
	/**
	 * key值jobStatus，value值为以jobUuid为key，jobId为value的map
	 */
	Map<String, Map<String, JobId>> jobIdMap = new HashMap<String, Map<String, JobId>>();
	/**
	 * key值为jobUuid，value值为jobName
	 */
	Map<String, String> jobSegmentMap = new HashMap<String, String>();
	/**
	 * 记录日志操作
	 */
	private static Logger logger = Logger.getLogger(JsonCompilation.class);

	/**
	 * 内部类，用以存放同一cube中的多个error状态jobUuid
	 * 
	 * @author Administrator
	 */
	class JobId {
		String JobId;

		public JobId(String JobId) {
			super();
			this.JobId = JobId;
		}
	}

	public JsonCompilation() {
		super();
		PropertyUtil.loadConfig();
	}

	/**
	 * 获取某个cube中的所有job的id
	 * 
	 * @param projectName
	 * @param cubeName
	 * @return
	 */
	public StringBuffer getJobId(String projectName, String cubeName, String params) {
		logger.info("获取某个cube下的job Id:" + "projectName: " + projectName + "  " + "cubeName:  " + cubeName);
		StringBuffer job = new StringBuffer();
		job = api.getJob(projectName, cubeName, params);
		return job;
	}

	/**
	 * 获取cubeLists
	 * 
	 * @return
	 */
	public StringBuffer getCubeLists(String params) {
		StringBuffer cube = new StringBuffer();
		cube = api.getList(params);
		return cube;
	}

	/**
	 * 获取系统时间并将其格式化
	 * 
	 * @return
	 */
	public String time() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
		systemTime = df.format(new Date());
		return systemTime;
	}

	/**
	 * 解析cubelist返回buf中的json数组
	 */
	public void compileCubeJson() {
		cubeBuf = getCubeLists(null);
		String cubeString = cubeBuf.toString();
		JSONArray cubeJsonArray = null;
		try {
			cubeJsonArray = new JSONArray(cubeString);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			logger.info(e.getMessage());
		}
		cSize = cubeJsonArray.length();
		for (int j = 0; j < cSize; j++) {
			JSONObject cubeJsonObj = null;
			try {
				cubeJsonObj = cubeJsonArray.getJSONObject(j);
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				logger.info(e1.getMessage());
			}
			String cubeName = null;
			try {
				cubeName = cubeJsonObj.get("name") + "";
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				logger.info(e.getMessage());
			}
			cubeSet.add(cubeName);
		}
	}

	/**
	 * 获取有关Kylin job的Json数组
	 * 
	 * @param projectName
	 * @param cubeName
	 */
	public void compileJobJson(String projectName, String cubeName) {
		String systemTime = time();
		jobBuf = getJobId(projectName, cubeName, null);
		String jobString = jobBuf.toString();
		JSONArray jobJsonArray = null;
		try {
			jobJsonArray = new JSONArray(jobString);
		} catch (JSONException e2) {
			// TODO Auto-generated catch block
			logger.info(e2.getMessage());
		}
		jSize = jobJsonArray.length();// 获得json数组的长度
		// 获得jobjson数组中，uuid、job_status、name后的值
		for (int i = 0; i < jSize; i++) {
			JSONObject jobJsonObj = null;
			try {
				jobJsonObj = jobJsonArray.getJSONObject(i);
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				logger.info(e1.getMessage());
			}
			String jobUuid = null;
			try {
				jobUuid = jobJsonObj.get("uuid") + "";
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				logger.info(e1.getMessage());
			}
			String jobStatus = null;
			try {
				jobStatus = jobJsonObj.get("job_status") + "";
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String jobName = null;
			try {
				jobName = jobJsonObj.get("name") + "";
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String jobLog = "[" + i + "]" + " " + systemTime + " " + "jobName: " + jobName + " " + "jobUuid= " + jobUuid
					+ "	 " + "jobStatus:" + " " + jobStatus + "\r\n";
			if (jobStatus.equals("ERROR")) {
				JobId jobId = new JobId(jobStatus);
				if (jobIdMap.get(jobStatus) == null) {
					jobIdMap.put(jobStatus, new HashMap<String, JobId>());
				}
				jobIdMap.get(jobStatus).put(jobUuid, jobId);
				jobSegmentMap.put(jobUuid, jobName);
				jobErrorBuf.append(jobLog + "\r\n");
			}
			jobLogBuf.append(jobLog + "\r\n");
		}
	}
}

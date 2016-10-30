package com.njws.checkjoberror;

import com.njws.checkjoberror.CompareOperation;
import com.njws.checkjoberror.FileOperation;
import com.njws.checkjoberror.JsonCompilation;
import com.njws.checkjoberror.KylinApi;
import com.njws.checkjoberror.TimeOperation;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.log4j.Logger;
import util.PropertyUtil;

public class JobError extends TimerTask {
	/**
	 * Refresh操作调用Kylin的返回buf值
	 */
	StringBuffer refreshBuffer = new StringBuffer();
	/**
	 * 新增操作调用Kylin的返回buf值
	 */
	StringBuffer addBuffer = new StringBuffer();
	/**
	 * 需要Refresh的数据
	 */
	Set<String> jobRefreshSet = new HashSet<String>();
	/**
	 * 需要新增数据的集合
	 */
	Set<String> jobAddSet = new HashSet<String>();
	/**
	 * key值segment开始时间，value值segment结束时间
	 */
	Map<String, String> jobRefreshMap = new HashMap<String, String>();
	/**
	 * Key值请求新增操作的时间，value值该时间的下一天
	 */
	Map<String, String> jobAddMap = new HashMap<String, String>();
	/**
	 * 实例化比较操作
	 */
	static CompareOperation judge = new CompareOperation();
	/**
	 * 实例化文件读写操作
	 */
	static FileOperation rw = new FileOperation();
	/**
	 * 实例化解析json数组
	 */
	static JsonCompilation json = new JsonCompilation();
	/**
	 * 实例化检查ERROR状态
	 */
	static JobError je = new JobError();
	/**
	 * 实例化时间操作
	 */
	static TimeOperation time = new TimeOperation();
	/**
	 * 实例化kylinApi类
	 */
	static KylinApi api = new KylinApi();
	/**
	 * 记录日志操作
	 */
	private static Logger logger = Logger.getLogger(JobError.class);

	/**
	 * 加载配置文件路径
	 */
	public JobError() {
		super();
		PropertyUtil.loadConfig();
	}

	/**
	 * Refresh操作
	 * 
	 * @param refreshStarttime
	 * @param refreshEndtime
	 * @param refreshCube
	 * @return refreshBuffer
	 */
	public StringBuffer refreshCube(String refreshStartTime, String refreshEndTime, String refreshCube) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		logger.info("Refresh操作: " + "开始时间: " + refreshStartTime + "   " + "结束时间: " + refreshEndTime + "   "
				+ "cubename: " + refreshCube);
		try {
			Date startTime = format.parse(refreshStartTime + " " + "08:00:00");
			Date endTime = format.parse(refreshEndTime + " " + "08:00:00");
			refreshBuffer = api.refresh(startTime, endTime, refreshCube);
		} catch (ParseException e) {
			logger.info(e.getMessage());
		}
		return refreshBuffer;
	}

	/**
	 * 新增操作
	 * 
	 * @param addStartTime
	 * @param addEndTime
	 * @param addCube
	 * @return addBuffer
	 */
	public StringBuffer addSegment(String addStartTime, String addEndTime, String addCube) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		logger.info(
				"新增操作: " + "开始时间: " + addStartTime + "   " + "结束时间: " + addEndTime + "   " + "cubename: " + addCube);
		try {
			Date startTime = format.parse(addStartTime + " " + "08:00:00");
			Date endTime = format.parse(addEndTime + " " + "08:00:00");
			addBuffer = api.add(startTime, endTime, addCube);
		} catch (ParseException e) {
			logger.info(e.getMessage());
		}
		return addBuffer;
	}

	/**
	 * Discard掉error状态job
	 * 
	 * @param jobId
	 * @param kylinIp
	 */
	public void discardJob(String jobId, String params) {
		api.discard(jobId, params);
	}

	/**
	 * 定时执行任务,拟定每30分钟运行一次
	 */
	public void run() {
		json.compileCubeJson();// 获取kylin中的所有cube
		rw.readConfigFile(PropertyUtil.prop.getProperty("configFile"));// 读取配置文件
		for (String cubeName : json.cubeSet) {
			if (rw.cubeProject.get(cubeName) != null) {
				logger.info("当前检测的项目名称:	" + rw.cubeProject.get(cubeName) + "  " + "当前检测的cube名称:" + cubeName);
				json.compileJobJson(rw.cubeProject.get(cubeName), cubeName);
				Map<String, JsonCompilation.JobId> UuidMap = json.jobIdMap.get("ERROR");
				if (UuidMap != null) {
					Set<String> jobIdSet = UuidMap.keySet();
					for (String jobIdSet1 : jobIdSet) {
						je.discardJob(jobIdSet1, null);
						String jobName = json.jobSegmentMap.get(jobIdSet1);
						logger.info("处于ERROR状态的job: " + jobIdSet1 + "  " + jobName);
						time.getSegmentTime(jobName);
						// 如果该error状态的job属于refresh操作，则进行refresh，否则进行addSegment操作
						if (judge.isSegmentExist(je.refreshCube(time.startTime, time.endTime, cubeName).toString()))
						{
							je.addSegment(time.startTime, time.endTime, cubeName);
						}
					}
				}
			}
		}
		rw.writeToLogFile(PropertyUtil.prop.getProperty("joberrorlog"), json.jobErrorBuf.toString()); // Error状态job信息持久化到日志
		rw.writeToLogFile(PropertyUtil.prop.getProperty("joblog"), json.jobLogBuf.toString());
		json.jobErrorBuf.setLength(0);
		json.jobLogBuf.setLength(0);

	}

	public static void main(String[] args) {
		Timer timer = new Timer();
		long delay = Long.parseLong(PropertyUtil.prop.getProperty("delay"));
		long period = Long.parseLong(PropertyUtil.prop.getProperty("period"));
		// 从现在开始 1 秒钟之后，每隔半小时执行一次 job
		timer.schedule(new JobError(), delay, period);
	}
}

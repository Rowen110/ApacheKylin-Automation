package com.njws.oadataimport;

import util.PropertyUtil;
import com.njws.oadataimport.FileOperation.MainTableInfo;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.log4j.Logger;
import java.util.*;

/**
 * 类的说明 类名：ImportationData 作者：柏晨浩 时间：2016年9月18日 类的功能：关于Kylin
 * Refresh以及build接口的调用以及忙时等待的业务逻辑设计
 */
public class ImportationData extends TimerTask {
	/**
	 * Refresh操作调用Kylin的返回buf值
	 */
	StringBuffer refreshBuffer = new StringBuffer();
	/**
	 * 新增操作调用Kylin的返回buf值
	 */
	StringBuffer addBuffer = new StringBuffer();
	/**
	 * 待持久化到setfile的忙时等待数据
	 */
	StringBuffer waitBuffer = new StringBuffer();
	/**
	 * 存放忙时等待数据的集合
	 */
	Set<String> waitSet = new HashSet<String>();
	/**
	 * 需要Refresh的数据
	 */
	Set<String> refreshSet = new HashSet<String>();
	/**
	 * 需要新增数据的集合
	 */
	Set<String> addSet = new HashSet<String>();
	/**
	 * key值segment开始时间，value值segment结束时间
	 */
	Map<String, String> refreshMap = new HashMap<String, String>();
	/**
	 * Key值请求新增操作的时间，value值该时间的下一天
	 */
	Map<String, String> addMap = new HashMap<String, String>();
	/**
	 * key值segment的开始时间，value值为该segment对应的Refresh的数据
	 */
	Map<String, Map<String, CompareTime>> timeMap = new HashMap<String, Map<String, CompareTime>>();
	/**
	 * 实例化计算当前时间下一天的外部类
	 */
	static TimeOperation time = new TimeOperation();
	/**
	 * 实例化读写文件的外部类
	 */
	static FileOperation rw = new FileOperation();
	/**
	 * 实例化存放比较判断方法的外部类
	 */
	static CompareOperation bool = new CompareOperation();
	/**
	 * 实例化解析Json字符串的外部类
	 */
	static JsonCompilation json = new JsonCompilation();
	/**
	 * 实例化KylinApi接口类
	 */
	static KylinApi api = new KylinApi();
	/**
	 * 记录日志操作
	 */
	private static Logger logger = Logger.getLogger(ImportationData.class);

	/**
	 * 导入配置文件路径
	 */
	public ImportationData() {
		super();
		PropertyUtil.loadConfig();
	}

	/**
	 * @author 柏晨浩 待操作数据的时间列
	 */
	class CompareTime {
		String DataTime;

		public CompareTime(String DataTime) {
			super();
			this.DataTime = DataTime;
		}
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
	 * 将经Kafka到HBase处理过后的数据分为需Refresh操作和需新增操作这两批
	 * 
	 * @param cubeName
	 * @param dataTime
	 */
	public void cleanData(String cubeName, String dataTime) {
		json.compileSegmentJson(cubeName);
		for (int j = 0; j < json.sSize; j++) {
			if (dataTime.compareTo(json.segStartTime[j]) >= 0 && dataTime.compareTo(json.segEndTime[j]) < 0) {
				refreshSet.add(json.segStartTime[j]);
				refreshMap.put(json.segStartTime[j], json.segEndTime[j]);
				CompareTime compareTime = new CompareTime(dataTime);
				if (timeMap.get(json.segStartTime[j]) == null) {
					timeMap.put(json.segStartTime[j], new HashMap<String, CompareTime>());
				}
				timeMap.get(json.segStartTime[j]).put(dataTime, compareTime);// 将不同cube的同一segment对应的数据时间存放至CompareTime中
			} else {
				String thisYear = time.getCurrentYearFirst();
				String nextYear = time.getNextYearFirst();
				addSet.add(thisYear);
				addMap.put(thisYear, nextYear);
			}
		}
	}

	/**
	 * Segment忙时等待设计，将存放等待数据的waitSet持久化到本地
	 */
	public void waitSegment() {
		for (String waiting : waitSet) {
			logger.info("需要等待的数据为：" + "  " + waiting);
			Map<String, FileOperation.MainTableInfo> mainTableMap = rw.initMap.get(waiting);
			if (mainTableMap != null) {
				Set<String> mainTableSet = mainTableMap.keySet();
				for (String mainTable : mainTableSet) {
					waitBuffer.append(waiting + " " + mainTable + "\r\n");
				}
			}
		}
		String waitString = waitBuffer.toString();
		logger.info(waitString);
		rw.writeToSetFile(PropertyUtil.prop.getProperty("setFile"), waitString);// 将Set集合中的内容持久化到磁盘文件
		rw.writeToSetFile(PropertyUtil.prop.getProperty("setFileBk"), waitString);

		// 清空相应的内容，包括buffer值、Set值以及Map
		waitBuffer.setLength(0);
		waitSet.clear();
		rw.timeSet.clear();
		refreshSet.clear();
		addSet.clear();
		refreshMap.clear();
		addMap.clear();
		rw.initMap.clear();
	}

	/**
	 * 定时执行的程序
	 */
	public void run() {
		logger.info("read properties");
		rw.readConfigFile(PropertyUtil.prop.getProperty("configFile"));// 读取配置文件
		rw.readDataFile(PropertyUtil.prop.getProperty("dataFile")); // 读取处理kafka数据生成的文件
		rw.readSetFile(PropertyUtil.prop.getProperty("setFile"));

		for (String timeSet1 : rw.timeSet) {
			Map<String, MainTableInfo> mainTableMap = rw.initMap.get(timeSet1);
			if (mainTableMap != null) {
				Set<String> mainTableSet = mainTableMap.keySet();
				for (String mainTable : mainTableSet) {
					logger.info("读取kafka文件: " + timeSet1 + "---------" + mainTable);
					if (rw.tableCube.get(mainTable) != null) {
						cleanData(rw.tableCube.get(mainTable), timeSet1);
					}
				}
			}
		}

		// 如果建模的表对应seg在cube中存在，则执行refresh操作
		for (String refreshSet1 : refreshSet) {
			Map<String, CompareTime> existMap = timeMap.get(refreshSet1);
			if (existMap != null) {
				Set<String> existSet = existMap.keySet();
				for (String existSet1 : existSet) {
					StringBuffer refreshBuf = new StringBuffer();
					Map<String, FileOperation.MainTableInfo> mainTableMap = rw.initMap.get(existSet1);
					if (mainTableMap != null) {
						Set<String> mainTableSet = mainTableMap.keySet();
						for (String mainTable : mainTableSet) {
							logger.info("需要刷新的数据：" + existSet1 + "---------" + rw.tableCube.get(mainTable));
							refreshBuf = refreshCube(refreshSet1, refreshMap.get(refreshSet1),
									rw.tableCube.get(mainTable));
							String refreshString = refreshBuf.toString();
							if (bool.isBusy(refreshString)) {
								logger.info("The time need to wait: " + refreshSet1);
								waitSet.add(refreshSet1);
							}
						}
					}
				}
			}
		}

		// 如果建模的表对应seg在cube中不存在，则执行新增build操作
		for (String addSet1 : addSet) {
			Map<String, FileOperation.MainTableInfo> mainTableMap = rw.initMap.get(addSet1);
			if (mainTableMap != null) {
				Set<String> mainTableSet = mainTableMap.keySet();
				for (String mainTable : mainTableSet) {
					StringBuffer addBuf = new StringBuffer();
					logger.info("需要新增的数据：" + addSet1 + "---------" + rw.tableCube.get(mainTable));
					addBuf = addSegment(addSet1, addMap.get(addSet1), rw.tableCube.get(mainTable));
					String addString = addBuf.toString();
					if (bool.isBusy(addString)) {
						logger.info("The time need to wait: " + addSet1);
						waitSet.add(addSet1);
					}
				}
			}
		}
		waitSegment();
	}

	/**
	 * 每隔1小时执行一次run方法
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Timer timer = new Timer();
		long delay = Long.parseLong(PropertyUtil.prop.getProperty("delay"));
		long period = Long.parseLong(PropertyUtil.prop.getProperty("period"));
		// 从现在开始 1 秒钟之后，每隔1小时执行一次 job1
		timer.schedule(new ImportationData(), delay, period);
	}
}

package com.njws.oadataimport;

import util.PropertyUtil;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.log4j.Logger;
import org.json.*;



/**类的说明
 * 类名：JsonCompilation
 * 作者：柏晨浩
 * 时间：2016年9月18日
 * 类的功能：调用kylin接口，对buf中的返回json串进行解析
 */
public class JsonCompilation {
	
		/**
		 * 存放getSegment 返回buf的json数组大小
		 */
			int sSize;
		/**
		 * 单个segment的开始时间
		 */
			String start;
		/**
		 * 单个segment的结束时间
		 */
			String end;
		/**
		 *一个cube中的所有segment的开始时间
		 */
			String[] segStartTime ;
		/**
		 * 一个cube中的所有segment的结束时间
		 */
			String[] segEndTime;
		/**
		 * 存放getSegment的返回buf
		 */
			StringBuffer segBuf = new StringBuffer();
		/**
		 * 实例化读写文件类
		 */
			FileOperation rw= new FileOperation();
		/**
		 * 实例化导入数据类
		 */
			static ImportationData idata = new ImportationData();
		/**
		 * 实例化KylinApi类
		 */
			static KylinApi api = new KylinApi();
		/**
		 * 记录日志操作
		 */
			private static Logger logger = Logger.getLogger(JsonCompilation.class);  

			public JsonCompilation() {
				super();
				PropertyUtil.loadConfig();
			}
/**
 * 	获取某个cube的信息，目的在于获取该cube已有的segment。
 * @param cubeName
 * @return
 */
public StringBuffer getSegment(String cubeName,String params) {
		logger.info("获取某个cube的segment:   "+"cubename:  "+cubeName);
		StringBuffer segment = new StringBuffer();
		segment=api.getSeg(cubeName,params);
	    return segment;
}


/**
 * 解析cube中Segment的json数组
 * @param cubeName
 */
public void  compileSegmentJson(String cubeName) {
		segBuf=getSegment(cubeName,null);
		String segString = segBuf.toString();
		//返回的buf内容并不是一个规范的json数组，这里需要截取一下
		String segSubString = segString.substring(segString.indexOf("[{\"uuid"),segString.indexOf(",\"last_modified\""));
		JSONArray segJsonArray = null;
		try {
			segJsonArray = new JSONArray(segSubString);
		} catch (JSONException e4) {
			// TODO Auto-generated catch block
			logger.info(e4.getMessage());
		}
		sSize = segJsonArray.length();//json数组的长度
		segStartTime=new String[sSize];//segment的开始时间
		segEndTime=new String[sSize];//segment的结束时间
		
		for (int i = 0; i < sSize; i++) {
			JSONObject segJsonObj = null;
			try {
				segJsonObj = segJsonArray.getJSONObject(i);
			} catch (JSONException e3) {
				// TODO Auto-generated catch block
				logger.info(e3.getMessage());
			}
			Calendar c = Calendar.getInstance();
			Calendar a = Calendar.getInstance();
			String segStartString = "";
			try {
				segStartString = segJsonObj.get("date_range_start")+"";
			} catch (JSONException e2) {
				// TODO Auto-generated catch block
				logger.info(e2.getMessage());
			}//segment中的开始时间对json数组中的date_range_start后的内容
			String segEndString = null;
			try {
				segEndString = segJsonObj.get("date_range_end")+"";
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				logger.info(e1.getMessage());
			}//segment中的结束时间对json数组中的date_range_end后的内容
			Long segStartLong = Long.valueOf(segStartString);
			Long segEndLong=Long.valueOf(segEndString);
			c.setTimeInMillis(segStartLong);
			a.setTimeInMillis(segEndLong);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date startDate= c.getTime();
			Date endDate = a.getTime();
			start = sdf.format(startDate);
			end = sdf.format(endDate);
			segStartTime[i]=start;
			segEndTime[i]=end;
			}
		}
}





package com.njws.checkjoberror;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;



/**类的说明
 * 类名：TimeOperation
 * 作者：柏晨浩
 * 时间：2016年9月18日
 * 类的功能：包含一些关于时间的方法
 */
public class TimeOperation{
		/**
		 * ERROR job对应segment的开始时间
		 */
		String startTime;
		/**
		 * ERROR job对应segment的结束时间
		 */
		String endTime;
		
/**
 * 获取处于error状态的job对应的segment
 * @param jobName
 */
public void getSegmentTime(String jobName){
	String jobString = jobName.substring(jobName.indexOf("-"),jobName.indexOf("T"));
	String jobStartTime = jobString.substring(2,10);//取得segment开始时间
	String jobEndTime = jobString.substring(17,25);//取得segment结束时间
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
	Date date1 = null;
	Date date2 = null;
	try {
		date1 = sdf.parse(jobStartTime);
		date2 = sdf.parse(jobEndTime);
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}			
	startTime = sdf1.format(date1);
	endTime = sdf1.format(date2);
	}
}


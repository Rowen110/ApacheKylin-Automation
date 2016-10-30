package com.njws.checkjoberror;

/**类的说明
 * 类名：CompareOperation
 * 作者：柏晨浩
 * 时间：2016年9月18日
 * 功能说明：关于需要比较的返回布尔值的方法实现
 */
public class CompareOperation {	
		
/**
 * 判断Segment是否存在，存在执行refresh操作，否则执行addSegment操作
 * @param RefreshBuf
 * @return
 */
public boolean isSegmentExist(String refreshBuf){
		String searchChars = "exception";
		return refreshBuf.contains(searchChars);
	}
}


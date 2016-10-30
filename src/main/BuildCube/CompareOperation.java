package com.njws.oadataimport;

/**类的说明
 * 类名：CompareOperation
 * 作者：柏晨浩
 * 时间：2016年9月18日
 * 功能说明：关于需要比较的返回布尔值的方法实现
 */
public class CompareOperation {	
		
/**
 * 	判断需要进行refresh或者build的segment现在是否正忙，具体判断依据为返回的buf中的值是否存在“overlap”字样。
 * @param returnString
 * @return
 */	
public boolean isBusy(String returnString){
		String searchChars="overlap";
		return returnString.contains(searchChars);
	}	
}


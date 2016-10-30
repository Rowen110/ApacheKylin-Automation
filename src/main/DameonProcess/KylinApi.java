package com.njws.checkjoberror;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Date;
import org.apache.commons.codec.binary.Base64;
import util.PropertyUtil;

public class KylinApi {
	/**
	 * 加载配置文件路径
	 */
	public KylinApi() {
		super();
		PropertyUtil.loadConfig();
	}

	static String encoding;
	StringBuffer refreshBuffer = new StringBuffer();

	/**
	 * 通过httpClient方式调用kylin Restful接口
	 * 
	 * @param kylinIp
	 * @param para
	 * @param method
	 * @param params
	 * @return
	 */
	public StringBuffer excute(String kylinIp, String para, String method, String params) {
		StringBuffer out = new StringBuffer();
		try {
			URL url = new URL("http://" + kylinIp + "/kylin/api" + para);
			System.out.println(url);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod(method);
			connection.setDoOutput(true);
			connection.setRequestProperty("Authorization", "Basic " + encoding);
			connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
			if (params != null) {
				byte[] outputInBytes = params.getBytes("UTF-8");
				OutputStream os = connection.getOutputStream();
				os.write(outputInBytes);
				os.close();
			}
			InputStream content = (InputStream) connection.getInputStream();
			// 解决乱码问题
			BufferedReader in = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
			String line;
			while ((line = in.readLine()) != null) {
				out.append(line);
			}
			in.close();
			connection.disconnect();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return out;
	}

	/**
	 * 登录kylin，输入身份认证，默认用户名为ADMIN，密码为KYLIN
	 * 
	 * @return
	 */
	public StringBuffer login() {
		login();
		String method = "POST";
		String para = "/user/authentication";
		byte[] key = ("ADMIN:KYLIN").getBytes();
		encoding = Base64.encodeBase64String(key);
		return excute(PropertyUtil.prop.getProperty("kylinIp"), para, method, null);
	}

	/**
	 * 获取cube下的所有job
	 * 
	 * @param cubeName
	 * @param params
	 * @return
	 */
	public StringBuffer getJob(String projectName, String cubeName, String params) {
		login();
		String method = "GET";
		String para = "/api/jobs/list/" + projectName + "/" + cubeName;
		return excute(PropertyUtil.prop.getProperty("kylinIp"), para, method, params);
	}

	/**
	 * 获取所有的cube名称
	 * 
	 * @param params
	 * @return
	 */
	public StringBuffer getList(String params) {
		login();
		String method = "GET";
		String para = "/cubes";
		return excute(PropertyUtil.prop.getProperty("kylinIp"), para, method, params);
	}

	/**
	 * Rebuild Cube
	 * 
	 * @param cubeName
	 * @param params
	 * @return
	 */
	public StringBuffer buildCube(String cubeName, String params) {
		String method = "PUT";
		String para = "/cubes/" + cubeName + "/rebuild";
		return excute(PropertyUtil.prop.getProperty("kylinIp"), para, method, params);
	}

	/**
	 * discard掉某个job
	 * 
	 * @param jobId
	 * @param params
	 */
	public void discard(String jobId, String params) {
		login();
		String method = "PUT";
		String para = "/jobs" + jobId + "/cancel";
		excute(PropertyUtil.prop.getProperty("kylinIp"), para, method, params);
	}

	/**
	 * refresh Cube
	 * 
	 * @param startTime
	 * @param endTime
	 * @param refreshCube
	 * @return
	 */
	public StringBuffer refresh(Date startTime, Date endTime, String refreshCube) {
		login();
		String body = "{\"startTime\":\"" + startTime.getTime() + "\", \"endTime\":\"" + endTime.getTime()
				+ "\", \"buildType\":\"REFRESH\"}";
		StringBuffer refreshResult = new StringBuffer();
		refreshResult.append(buildCube(refreshCube, body));
		return refreshResult;
	}

	/**
	 * add Segment
	 * 
	 * @param startTime
	 * @param endTime
	 * @param addCube
	 * @return
	 */
	public StringBuffer add(Date startTime, Date endTime, String addCube) {
		login();
		String body = "{\"startTime\":\"" + startTime.getTime() + "\", \"endTime\":\"" + endTime.getTime()
				+ "\", \"buildType\":\"BUILD\"}";
		StringBuffer addResult = new StringBuffer();
		addResult.append(buildCube(addCube, body));
		return addResult;
	}
}

package com.njws.getmaintabletime;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;

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
		static final String KYLIN_LIMIT=PropertyUtil.prop.getProperty("limit");

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
		public StringBuffer login() {
			String method = "POST";
			String para = "/user/authentication";
			byte[] key = ("ADMIN:KYLIN").getBytes();
			encoding = Base64.encodeBase64String(key);
			return excute(PropertyUtil.prop.getProperty("kylinIp"), para, method, null);
		}
		
		public StringBuffer query(String params) {
			String method = "POST";
			String para = "/query";
			return excute(PropertyUtil.prop.getProperty("kylinIp"),para,method, params);
		}
		
		public StringBuffer getTableTime(String sql,String lookupId,String projectName){
			login();
			String body = "{\"sql\": \""+ sql +lookupId+"\",\"offset\":0,\"limit\":" + KYLIN_LIMIT + ",\"acceptPartial\":false, \"project\":\"" +projectName+ "\"}";
			StringBuffer queryResult = query(body);
			return queryResult;
		}
}

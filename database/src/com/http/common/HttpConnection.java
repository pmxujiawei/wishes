package com.http.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.lang.StringUtils;

/**
 * http����ģ��
 * 
 * @author xujw
 * 
 */
public class HttpConnection {

	public static final String URL = "http://hq.sinajs.cn/list=sz150210";

	/** ��������Ĭ�ϱ��� */
	public static final String ENCODE_DEFAULT = "UTF-8";

	public static final String POST = " POST ";

	/**
	 * ģ��get�����ȡ��������
	 * 
	 * @param requestUrl
	 *            get����url
	 * @return ��������
	 * @throws IOException
	 */
	public static String requestForGet(String requestUrl, String encode)
			throws IOException {

		// ƴ��get�����URL�ִ���ʹ��URLEncoder.encode������Ͳ��ɼ��ַ����б���

		// String getURL = GET_URL + " ?username= "
		//
		// + URLEncoder.encode("fat man", " utf-8 ");
		URL getUrl = new URL(requestUrl);

		// ����ƴ�յ�URL�������ӣ�URL.openConnection()���������
		// URL�����ͣ����ز�ͬ��URLConnection����Ķ������������ǵ�URL��һ��http�������ʵ���Ϸ��ص���HttpURLConnection

		HttpURLConnection connection = (HttpURLConnection) getUrl
				.openConnection();

		// ����������������ӣ���δ��������
		connection.connect();

		// �������ݵ���������ʹ��Reader��ȡ���ص�����
		if (StringUtils.isEmpty(encode)) {
			encode = ENCODE_DEFAULT;
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				connection.getInputStream(), encode));

		StringBuffer sb = new StringBuffer();
		String lines;
		while ((lines = reader.readLine()) != null) {
			sb.append(lines);
		}
		reader.close();
		// �Ͽ�����
		connection.disconnect();
		return sb.toString();
	}

	/**
	 * ģ��post����
	 * 
	 * @param requestUrl
	 * @param encode
	 * @return
	 * @throws IOException
	 */
	public static String requestForPost(String requestUrl, String encode)
			throws IOException {

		// Post�����url����get��ͬ���ǲ���Ҫ������

		URL postUrl = new URL(requestUrl);

		// ������

		HttpURLConnection connection = (HttpURLConnection) postUrl
				.openConnection();

		// �򿪶�д���ԣ�Ĭ�Ͼ�Ϊfalse
		connection.setDoOutput(true);

		connection.setDoInput(true);

		// ��������ʽ��Ĭ��ΪGET
		connection.setRequestMethod(POST);

		// Post ������ʹ�û���
		connection.setUseCaches(false);

		connection.setInstanceFollowRedirects(true);

		// �������ӵ�Content-type������Ϊapplication/x-
		// www-form-urlencoded����˼��������urlencoded�������form�������������ǿ��Կ������Ƕ���������ʹ��URLEncoder.encode���б���
		connection.setRequestProperty(" Content-Type ",
				" application/x-www-form-urlencoded ");

		// ���ӣ���postUrl.openConnection()���˵����ñ���Ҫ�� connect֮ǰ��ɣ�

		// Ҫע�����connection.getOutputStream()�������Ľ��е��� connect()�������������ʡ��

		// connection.connect();

		// DataOutputStream out = new DataOutputStream(connection
		//
		// .getOutputStream());

		// ����������ʵ��get��URL��'?'��Ĳ����ַ���һ��

		// String content = " firstname= "
		// + URLEncoder.encode(" һ������� ", " utf-8 ");

		// DataOutputStream.writeBytes���ַ����е�16λ�� unicode�ַ���8λ���ַ���ʽд��������

		// out.writeBytes(content);
		//
		// out.flush();
		//
		// out.close(); // flush and close

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				connection.getInputStream()));

		String line;
		StringBuffer sb = new StringBuffer();
		while ((line = reader.readLine()) != null) {

			sb.append(line);

		}

		reader.close();

		// connection.disconnect();
		return sb.toString();

	}

	public static void main(String[] args) {

		// TODO Auto-generated method stub
		while (true) {
			try {

				// readContentFromPost();

				Thread.sleep(2000);
				// readContentFromPost();

			} catch (Exception e) {

				e.printStackTrace();

			}
		}

	}

}

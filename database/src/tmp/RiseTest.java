package tmp;

import com.http.common.HttpConnection;

public class RiseTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String requestUrl = "http://hq.sinajs.cn/list=sz150210";
		while (true) {
			System.out.println(HttpConnection.requestForGet(requestUrl, "gbk"));
			Thread.sleep(2000);
		}

	}

}

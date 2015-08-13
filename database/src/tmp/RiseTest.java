package tmp;

import com.http.common.HttpConnection;

public class RiseTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String requestUrl = "http://finance.sina.com.cn/data/";
		// while (true) {
		System.out.print(HttpConnection.requestForGet(requestUrl, "gbk"));
		// Thread.sleep(2000);
		// }

	}

}

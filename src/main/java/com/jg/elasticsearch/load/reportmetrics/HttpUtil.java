package com.jg.elasticsearch.load.reportmetrics;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;

public class HttpUtil {

	public static String get(String url){
		HttpClient client = new HttpClient();
		HttpMethod method = new GetMethod(url);
		try {
			int statusCode = client.executeMethod(method);

		      if (statusCode != HttpStatus.SC_OK) {
		        System.err.println("Method failed: " + method.getStatusLine());
		      }
		      byte[] responseBody = method.getResponseBody();
		      return new String( responseBody ) ;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null ; 
	}
}

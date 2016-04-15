package com.jg.elasticsearch.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.jg.util.dataholder.IDataHolder;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.Health;
import io.searchbox.cluster.NodesStats;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;

/**
 * 
 *
 */
public class RestClient {

	public JestClient createRestClient(IDataHolder config){
		
	    String clusterName = config.getStringValue("loadconfig", new String[]{"elasticsearch","cluster.name"});
	    List<String> ipList = config.getStringValues("loadconfig", new String[]{"elasticsearch","cluster_host_ips"});
	    String hostName = ipList.get(0);
		String portNumber = config.getStringValue("loadconfig", new String[]{"elasticsearch","portnumber"});
		
		config.getIntegerValue("loadconfig", new String[]{"metricscapture","timeinterval_fetch_seconds"});
		
		
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(
				new HttpClientConfig.Builder(
				String.format("http://%s:%s", hostName, portNumber))
				.multiThreaded(true)
				.maxTotalConnection(100)
				.defaultMaxTotalConnectionPerRoute(100)
				.connTimeout(180000)
				.readTimeout(180000).build()
				);
		JestClient client = factory.getObject();
		
		return client;
		
	}
	
	public List<JestClient> createMultipleRestClients(IDataHolder config){
		
		
	    List<String> ipList = config.getStringValues("loadconfig", new String[]{"elasticsearch","cluster_host_ips"});
	    
		String portNumber = config.getStringValue("loadconfig", new String[]{"elasticsearch","portnumber"});
		
		
		List<JestClient> listOfClients = new ArrayList<JestClient>();
		
		if( ipList != null ){
			for( String hostName : ipList ){	
					JestClientFactory factory = new JestClientFactory();
					factory.setHttpClientConfig(
							
							new HttpClientConfig.Builder(
							String.format("http://%s:%s", hostName, portNumber))
							.multiThreaded(true)
							.connTimeout(180000)
							.readTimeout(180000).build());
					JestClient client = factory.getObject();
					listOfClients.add( client );
					
			}
		}
		
		return listOfClients;
		
	}

	
	public static void main(String[] args) {
		
		//===== Input taken from a configuration =========//
		String hostName = "";
		String portNumber="9200";
		String indexName = "logtest3";
		
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(
				
				new HttpClientConfig.Builder(
				String.format("http://%s:%s", hostName, portNumber))
				.multiThreaded(true).build());
		JestClient client = factory.getObject();
//		try {
//			client.execute( 
//					new CreateIndex.Builder(indexName).build());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			return;
//		}
		
		try {
			JestResult resultHealth = client.execute(new Health.Builder().build());
			System.out.println( resultHealth.getJsonString());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
				
		
//		JestResult result = client.execute(new NodesStats.Builder()
//                .addNode(firstNode)
//                .withClear()
//                .withIndices()
//                .withJvm()
//                .build());
		

//		while (true) {
//			Bulk.Builder builder = new Bulk.Builder();
//			for (LogData obj : getList()) {
//				builder.addAction(new Index.Builder(obj).index(indexName).type(indexName).build());
//			}
//			try {
//				client.execute(builder.build());
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
	}

	public static List<LogData> getList( int numDocs) {
		List<LogData> data = new ArrayList<LogData>();
		for (int i = 0; i < numDocs; i++) {
			data.add(new LogData());
		}
		return data;

	}

	private static class LogData {

		public String getLogid() {
			return logid;
		}

		public void setLogid(String logid) {
			this.logid = logid;
		}

		public String getDataLog() {
			return dataLog;
		}

		public void setDataLog(String dataLog) {
			this.dataLog = dataLog;
		}

		String logid = UUID.randomUUID().toString();
		String dataLog = "Log Test data  " + logid
				+ " test data check if the result is already being used; if it's not already taken, it's suitable as a unique identifiercheck if the result is already being used; if it's not already taken, it's suitable as a unique identifiercheck if the result is already being used; if it's not already taken, it's suitable as a unique identifiercheck if the result is already being used; if it's not already taken, it's suitable as a unique identifiercheck if the result is already being used; if it's not already taken, it's suitable as a unique identifiercheck if the result is already being used; if it's not already taken, it's suitable as a unique identifiercheck if the result is already being used; if it's not already taken, it's suitable as a unique identifiercheck if the result is already being used; if it's not already taken, it's suitable as a unique identifier";

	}

}

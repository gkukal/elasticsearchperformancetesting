package com.jg.elasticsearch.load;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;

import com.codahale.metrics.Timer.Context;
import com.google.common.base.Stopwatch;
import com.jg.elasticsearch.client.RestClient;
import com.jg.elasticsearch.load.LoadDataToElasticSearch.BulkLoadTask;
import com.jg.elasticsearch.load.LoadDataToElasticSearch.LogDataFromProduction;
import com.jg.elasticsearch.load.LoadDataToElasticSearch.SerialKillerTask;
import com.jg.elasticsearch.load.reportmetrics.FetchESMetrics;
import com.jg.elasticsearch.load.reportmetrics.MetricHelper;
import com.jg.elasticsearch.util.ESUtil;
import com.jg.util.dataholder.GenericDataHolder;
import com.jg.util.dataholder.IDataHolder;

import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.BulkResult.BulkResultItem;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.aliases.RemoveAliasMapping;
import io.searchbox.core.Index;
import io.searchbox.params.Parameters;

public class LoadDataWithProductionDumpMessages {

	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(LoadDataToElasticSearch.class);

	public static void main(String[] args) {

		String fileName = null;
		String readFileToString = null;
		if (args != null && args.length > 0) {
			fileName = args[0];
			readFileToString = getFileAsString(fileName, readFileToString);

			IDataHolder config = new GenericDataHolder(readFileToString);
			logger.info(config != null ? config.getJSONData() : "NO config obj created. Returning ");
			if (config.getJSONRootNode() == null)
				return;
			try {
				
				//loadproduction
				String pathToLoadData = 
						config.getStringValue("loadconfig", new String[] { "loadproduction", "path" });
				Path path = Paths.get(pathToLoadData);
				List<String> file = java.nio.file.Files.readAllLines( path);
				loadedProductionMessages = file ;
				MetricHelper.initialize(config);
				LoadDataWithProductionDumpMessages loader = new LoadDataWithProductionDumpMessages();
				loader.loadData(config);
			} catch (Exception e) {
				logger.error("Error while loading data to ES ", e);
			}

		} else {
			logger.error("No config provided so returning. Provide full file path as argument to Main class ");
			return;
		}

	}

	public static String getFileAsString(String fileName, String readFileToString) {
		logger.info("Given Filepath for the config : " + fileName);
		try {
			readFileToString = FileUtils.readFileToString(new File(fileName));
			logger.info("Config file content : " + readFileToString);
		} catch (Exception e) {
			logger.error("Error while reading config file " + fileName, e);
		}
		return readFileToString;
	}
	public static boolean createANewIndex( JestClient restClient , String indexName , String typeName){
		//create a new index by creating some payload

		
		Index builder = new Index.Builder("{\"data\":\"Hello world\"}")
        .index(indexName)
        .type(typeName)
        .build();
		
		try {
			DocumentResult results = restClient.execute(builder);
			if( results.getResponseCode()  == 201 ) {
				logger.info( "New document created to Index "+indexName );
				return true;
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	
	public void createAlias( JestClient client , String indexName , String alias) throws IOException{
        ModifyAliases modifyAliases = new ModifyAliases.Builder( new RemoveAliasMapping.Builder( indexName+"*" , alias ).build()).build();
        JestResult result = client.execute(modifyAliases);
		AddAliasMapping addAliasMapping = new AddAliasMapping
                .Builder( indexName, alias )
                .build();
		ModifyAliases modifyAliasesAdd = new ModifyAliases.Builder(addAliasMapping).build();
		JestResult execute = client.execute(modifyAliasesAdd);
		System.out.println( "done");
        
	}
	
	public static void modifyAlias( JestClient client , String oldIndexName , String newIndexName , String alias) {
        
		try {
			//GetAliases modifyAliases = new GetAliases.Builder( new RemoveAliasMapping.Builder( oldIndexName , alias ).build()).build();
			
			ModifyAliases modifyAliases = new ModifyAliases.Builder( new RemoveAliasMapping.Builder( oldIndexName , alias ).build()).build();
			JestResult result = client.execute(modifyAliases);
			AddAliasMapping addAliasMapping = new AddAliasMapping
			        .Builder( newIndexName, alias )
			        .build();
			ModifyAliases modifyAliasesAdd = new ModifyAliases.Builder(addAliasMapping).build();
			client.execute(modifyAliasesAdd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void loadData(IDataHolder config) {

		boolean isRestClient = true;
		// Create client based on the defined config
		if (isRestClient) {

		}
		// Create metrics captured and writer thread
		metricsCreationThread(config);
		// Start the load process.
		Stopwatch watch = Stopwatch.createStarted();
		RestClient client = new RestClient();
		JestClient restClient = client.createRestClient(config);
		if (isRestClient) {
			Integer experimentTimeInSeconds = config.getIntegerValue("loadconfig",
					new String[] { "experiment", "run_for_seconds" });
			Integer experimentTimePerIndex = config.getIntegerValue("loadconfig",
					new String[] { "experiment", "changeIndexAfter_these_many_seconds" });
			String indexName = config.getStringValue("loadconfig", new String[] { "elasticsearch", "indexName" });
			String typeName = config.getStringValue("loadconfig", new String[] { "elasticsearch", "typeNameOfIndex" });
			String aliasNameIndex = indexName+"indexAlias";
			
			
			// Create a new Index 
			createANewIndex(  restClient , indexName , typeName);
			
			//create an alias with this alias
			try {
				createAlias(restClient, indexName, aliasNameIndex);
				logger.info(" Created an index alias (index/alias) :( " + indexName +"/" +aliasNameIndex +" ) ");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
//			//create thread to modify alias every N seconds as defined in config
//			ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(1);
//			scheduledPool.scheduleWithFixedDelay(
//					new CreateAliasTask( restClient ,indexName , aliasNameIndex , typeName , sharedIndexName ), experimentTimePerIndex,  experimentTimePerIndex , TimeUnit.SECONDS);
//			
			
			//Threads to put load 
			ExecutorService executor = null;
			executor = startTheLoadProcessUsingRestClient(config , restClient , indexName , typeName , aliasNameIndex);
			// =====timer to stop shutdown everything at the end of time duration
			// ========================================================================
			//Threads to kill all the threads and shut down
			Timer timerToStop = new Timer();
			timerToStop.schedule( new SerialKillerTask( executor),experimentTimeInSeconds * 1000);
					
//			Timer timerToStopKill = new Timer();
//			timerToStop.schedule( new SerialKillerTask( scheduledPool),experimentTimeInSeconds * 1000);
			
			}
	}
	
	public class SerialKillerTask extends TimerTask {
		ExecutorService m_executor = null ;

		public SerialKillerTask( ExecutorService executor ){
			 m_executor = executor ;
		}
	
		@Override
		public void run() {
			m_executor.shutdownNow();
			int cnt = 0;
				try {
					m_executor.awaitTermination(40000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			System.out.println( "Shutting down all threads. Test is over ");	
			System.exit(0);	
		}
	 }

	public void metricsCreationThread(IDataHolder config) {

		Integer delay = config.getIntegerValue("loadconfig",
				new String[] { "metricscapture", "timeinterval_fetch_seconds" });
		if (delay == null)
			delay = 60;
		ScheduledExecutorService scheduledService = Executors.newScheduledThreadPool(1);
		FetchESMetrics.FetchMetricsTask task = new FetchESMetrics.FetchMetricsTask(config);
		scheduledService.scheduleWithFixedDelay(task, 0, 60, TimeUnit.SECONDS);

	}

	public ExecutorService startTheLoadProcessUsingRestClient( IDataHolder config , JestClient restClient , String indexName , String typeName , String aliasName ) {
		// Integer delay = config.getIntegerValue("loadconfig", new
		// String[]{"metricscapture","timeinterval_fetch_seconds"});
		
		Integer threads = config.getIntegerValue("loadconfig", new String[] { "restclient", "threads" });
		if (threads == null)
			threads = 1;

		Stopwatch startTime = Stopwatch.createStarted();

		final ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(threads);
		Integer numberOfDocs = config.getIntegerValue("loadconfig",
				new String[] { "restclient", "load", "docs_in_batch" });
		Integer throttlingTimeBetweenRequests = config.getIntegerValue("loadconfig",
				new String[] { "restclient", "sleeptimebetweencalls_in_msec" });
		if (throttlingTimeBetweenRequests == null)
			throttlingTimeBetweenRequests = 100;

		// =====//
		// Get the shardID to IP of the node.
		String routingValueForThisShardId = null;
		routingValueForThisShardId =  getRoutingForThisIP( config , indexName );
		Boolean useRouting = config.getBooleanValue("loadconfig", new String[] { "restclient", "load", "useRouting" });
		Boolean overseerneeded = config.getBooleanValue("loadconfig", new String[] { "restclient", "overseerthread" });
		if( overseerneeded == null ) overseerneeded = false;
		// ========================================================================
		try {
			for (int i = 0; i < threads; i++) {
				BulkLoadTask task = null;
				if (!useRouting) {
					if( i == 0 && overseerneeded ){
						task = new BulkLoadTask(config , restClient, indexName, typeName, numberOfDocs, null, aliasName , true);
					}else{
						task = new BulkLoadTask(config , restClient, indexName, typeName, numberOfDocs, null, aliasName , false );
					}
				} else {
					if( i == 0 && overseerneeded ){
						task = 
								new BulkLoadTask(config ,restClient, indexName, typeName, numberOfDocs, routingValueForThisShardId , aliasName, true );
					}else{
						task = 
								new BulkLoadTask(config ,restClient, indexName, typeName, numberOfDocs, routingValueForThisShardId , aliasName, false );
					}
				}
				Future<?> submit = newFixedThreadPool.submit(task);
				
				// listOfresults.add(resultFuture);
			}
		} catch ( Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return newFixedThreadPool;
		
	}

	public static  List<String> loadedProductionMessages  = null;
	
	public static String getRoutingForThisIP(IDataHolder config , String indexName ){
		String routingValueForThisShardId = null;
		Boolean useRouting = config.getBooleanValue("loadconfig", new String[] { "restclient", "load", "useRouting" });
		if (useRouting != null && useRouting) {
			List<String> ipList = config.getStringValues("loadconfig",
					new String[] { "elasticsearch", "cluster_host_ips" });
			String hostIP = ipList.get(0);
			String portNumber = config.getStringValue("loadconfig", new String[] { "elasticsearch", "portnumber" });

			String shardIDForThisIPAndIndex = ESUtil.getShardIDForThisIPAndIndex(hostIP, portNumber, indexName);
			if( shardIDForThisIPAndIndex == null ) throw new RuntimeException("Issue in getting shards because Index "+ indexName +" not there on IP "+hostIP);
			// Get the routing value
			Integer numberOfShards = config.getIntegerValue("loadconfig",
					new String[] { "elasticsearch", "numberOfShards" });

			if (numberOfShards != null) {
				routingValueForThisShardId = ESUtil.routingValueForThisShardId( shardIDForThisIPAndIndex,
						numberOfShards);
				
				logger.info(String.format(
						"Shard generation :(host_ip /index_name/shard_id/number_of_shards/routingValueForThisShardId) --(%s/%s/%s/%s/%s) ",
						hostIP, indexName, shardIDForThisIPAndIndex, numberOfShards + "", routingValueForThisShardId));
				return routingValueForThisShardId;
			}
			
		}
		return null;
	}
	
	public static class BulkLoadTask implements Runnable {
		private static final Logger logger = org.slf4j.LoggerFactory.getLogger(BulkLoadTask.class);

		String m_indexName = null;
		String m_type = null;
		JestClient m_client = null;
		int m_numberOfDocs = 2000;
		String m_routingValue = null;
		String m_ipAddress = "IPnotknown";
		IDataHolder m_config = null ;
		String m_aliasName  = null ;
		boolean m_overseer  = false;
		

		private BulkLoadTask( IDataHolder config , JestClient client, String indexName, String type, 
				int numberOfDocs, String routingValue ,String aliasName , boolean overseer) {
			m_indexName = indexName;
			m_type = type;
			m_numberOfDocs = numberOfDocs;
			m_client = client;
			m_routingValue = routingValue;
			m_config  = config ;
			m_aliasName = aliasName ;
			m_overseer = overseer;
			try {
				m_ipAddress = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		}

		private static Random rnd = new Random();
		public static boolean onceAWhileIsTrue() {
			return rnd.nextBoolean() && rnd.nextBoolean() && rnd.nextBoolean();
		}

		public void run() {
			MetricHelper metricRegistry = MetricHelper.metricRegistry();
			int cnt = 1;
			logger.info(" Messages in dump"+ loadedProductionMessages.size());
			boolean toggle = false;
			Stopwatch watch = Stopwatch.createStarted();
			String currentIndexName  = m_indexName ;
			String prevPhysicalIndexName = m_indexName ;
			Integer experimentTimePerIndex = m_config.getIntegerValue("loadconfig", new String[] { "experiment", "changeIndexAfter_these_many_seconds" });
			
			
			while (true) {
				if( watch.elapsed(TimeUnit.SECONDS) > experimentTimePerIndex ){
					if( m_overseer ){
					logger.info( String.format(" Going to create alias for new Index and switching to new Index "));
					//change alias mapping
					   String newNameofIndex =  m_indexName+System.currentTimeMillis() ;
					   prevPhysicalIndexName = currentIndexName ;
					   currentIndexName = newNameofIndex ;
					   boolean createdIndex =  createANewIndex( m_client , currentIndexName , m_type );
					   logger.info( String.format(" New Index created ( indexname/OldIndexName) :( %s/%s) ", currentIndexName , prevPhysicalIndexName )); 
					   modifyAlias( m_client, prevPhysicalIndexName, currentIndexName , m_aliasName);
					   logger.info( String.format(" Alias modified for Index created ( indexname)  :( %s ) ", currentIndexName )); 
						//
						if( m_routingValue != null ){
							m_routingValue = getRoutingForThisIP( m_config , currentIndexName );
							logger.info( String.format(" Routing Value found for new Index Created  Index created ( indexname/routingValue)  :( %s/%s ) ", currentIndexName,m_routingValue )); 
						}
						watch.reset();
						watch.start();
					}
				}
				
				Context startTimer = metricRegistry.startTimer(m_ipAddress, "bulkcallTime");
				toggle = !toggle;
				try {
					metricRegistry.addToMeter(new Long(m_numberOfDocs),
							new String[] { m_ipAddress, "request", "total" });
					// TODO Auto-generated method stub
					Bulk.Builder builder = new Bulk.Builder().defaultIndex(m_aliasName).defaultType(m_type);

					List<BulkableAction<?>> listOfBulkActions = new ArrayList<BulkableAction<?>>();
					String messageFromProductionDump = null;
					
					if( cnt > loadedProductionMessages.size() -1 ){
						cnt = 0;
					}
					Stopwatch startTimeCreateRequest = Stopwatch.createStarted();
					
					if( toggle && cnt < loadedProductionMessages.size()  ){
						for (int i = 0 ; i < m_numberOfDocs ; i++ ) {
							messageFromProductionDump = loadedProductionMessages.get(cnt);
							//LogDataFromProduction obj = new LogDataFromProduction( messageFromProductionDump );
							Index singleItem = null;
							if (m_routingValue == null) {
								singleItem = new Index.Builder(messageFromProductionDump ).build();
							} else {
								singleItem = new Index.Builder( messageFromProductionDump ).setParameter(Parameters.ROUTING, m_routingValue)
										.build();
								// logger.info("Using routing"+ m_routingValue +
								// singleItem.getURI() );
							}
							cnt++;
							listOfBulkActions.add(singleItem);
						}
					}else{
						List<LogDataMimickProduction> list = getList(m_numberOfDocs);
						for (LogDataMimickProduction obj : list) {
							Index singleItem = null;
							if (m_routingValue == null) {
								singleItem = new Index.Builder(obj).build();
							} else {
								singleItem = new Index.Builder(obj).setParameter(Parameters.ROUTING, m_routingValue)
										.build();
								// logger.info("Using routing"+ m_routingValue +
								// singleItem.getURI() );
							}

							listOfBulkActions.add(singleItem);
						}
					}
					startTimeCreateRequest.stop();
					logger.info( "Time Taken in making bulk call (ms) "+ startTimeCreateRequest.elapsed(TimeUnit.MILLISECONDS));
					
					builder.addAction(listOfBulkActions);
					int passed = 0;
					int totalRequested = listOfBulkActions.size();
					int totalResponse = 0;

					Stopwatch startTime = Stopwatch.createStarted();

					Bulk build = builder.build();
					BulkResult results = m_client.execute(build);
					startTimer.stop();
					long elapsed = startTime.elapsed(TimeUnit.SECONDS);
					if (results != null && results.getItems() != null && results.getItems().size() > 0) {
						logger.info( "Time to make call BULK call which has data " + elapsed + " seconds" );
					} else {
						logger.info( "Time to make call BULK call which does not have data " + elapsed + " seconds" );
					}

					startTime.stop();
					if (logger.isDebugEnabled()) {
						logger.info( " Single bulk call made, Response code:  " + results.getResponseCode()
								+ " Data back : " + results.getJsonString() );
					}
					Stopwatch startTimeResponse = Stopwatch.createStarted();
					for ( BulkResultItem single : results.getItems()) {
							totalResponse++;
							if ( single.status == 201 ){
								passed++;
							}else{
								logger.error("Error code "+single.status+ " "+ single.error );
							}
								
					}
					startTimeResponse.stop();
					logger.info("Time taken to read response (ms) "+ startTimeResponse.elapsed(TimeUnit.MILLISECONDS));
					
					logger.info(String.format(
								"Response stats ( TotalRequests/TotalResponses/TotalGoodResponses ) ( %s/%s/%s ) ",
								totalRequested, totalResponse, passed));
						metricRegistry.addToMeter( new Long( passed ) ,
								new String[] { m_ipAddress, "response", "requests_passed" });
						if ( passed < totalRequested ) {
							logger.error(
									String.format( "Issue in request response, Failed :" + (totalRequested - passed)));
						}
				} catch (Exception e) {
					logger.error("Error while making bulk call ", e);
					startTimer.stop();
				}
			}
		}

	}

	public static List<LogDataMimickProduction> getList(int numberOfDocs) {
		List<LogDataMimickProduction> data = new ArrayList<LogDataMimickProduction>();
		for (int i = 0; i < numberOfDocs; i++) {
			data.add(new LogDataMimickProduction());
		}
		if (logger.isDebugEnabled())
			logger.debug("Length of each BULK call " + data.size());
		return data;

	}

	public static class LogDataMimickProduction {
		@Override
		public String toString() {
			return "LogDataFromProduction [topTxnId=" + transactionId + ", appVersion=" + appNameVersion 
				    + ", groupId=" + groupId + ", sessionId=" + sessionId
					+ ", msgSubType=" + msgSubType + ", serverId=" + serverId + ", duration=" + duration
					+ ", parentTxnId=" + parentTxnId + ", payload=" + payload 
					+ ", epochTime=" + epochTime + ", categoryId=" + categoryId + "]";
		}

		public String getTopTxnId() {
			return transactionId;
		}

		public void setTopTxnId(String topTxnId) {
			this.transactionId = topTxnId;
		}

		public String getAppVersion() {
			return appNameVersion;
		}

		public void setAppVersion(String appVersion) {
			this.appNameVersion = appVersion;
		}

		public String getMsgType() {
			return msgType;
		}

		public void setMsgType(String msgType) {
			this.msgType = msgType;
		}

		public String getGroupId() {
			return groupId;
		}

		public void setGroupId(String groupId) {
			this.groupId = groupId;
		}

		public String getSessionId() {
			return sessionId;
		}

		public void setSessionId(String sessionId) {
			this.sessionId = sessionId;
		}

		public String getMsgSubType() {
			return msgSubType;
		}

		public void setMsgSubType(String msgSubType) {
			this.msgSubType = msgSubType;
		}

		public String getServerId() {
			return serverId;
		}

		public void setServerId(String serverId) {
			this.serverId = serverId;
		}

		public long getDuration() {
			return duration;
		}

		public void setDuration(long duration) {
			this.duration = duration;
		}

		public String getParentTxnId() {
			return parentTxnId;
		}

		public void setParentTxnId(String parentTxnId) {
			this.parentTxnId = parentTxnId;
		}

		public String getPayload() {
			return payload;
		}

		public void setPayload(String payload) {
			this.payload = payload;
		}


		public long getEpochTime() {
			return epochTime;
		}

		public void setEpochTime(long epochTime) {
			this.epochTime = epochTime;
		}

		public String getCategoryId() {
			return categoryId;
		}

		public void setCategoryId(String categoryId) {
			this.categoryId = categoryId;
		}

		String transactionId = UUID.randomUUID().toString();
		String appNameVersion = "1.2." + Math.random() + "";
		String msgType = "TN_" + RandomStringUtils.randomAlphabetic(2);
		String groupId = "Group" + RandomStringUtils.randomAlphabetic(2);
		String sessionId = "sessionId" + RandomStringUtils.randomAlphabetic(2);
		String msgSubType = "msgSubType" + RandomStringUtils.randomAlphabetic(2);
		String serverId = "serverId" + transactionId;
		long duration = System.currentTimeMillis();
		String parentTxnId = "parentTxnId" + UUID.randomUUID().toString();
		String payload = "Some Random data " + RandomStringUtils.randomAlphabetic(100) + " dataCenterID : prod"
				+ msgSubType + " txnLevel :" + Math.random() + "Some Random data "
				+ RandomStringUtils.randomAlphabetic(100);

		long epochTime = System.currentTimeMillis() * 1000;
		String categoryId = "Cat" + RandomStringUtils.randomAlphabetic(2);
	}
	
	
	public static class LogDataFromProduction {
		
		private static final ObjectMapper mapper = new ObjectMapper();
		
		HashMap<String,Object> m_result = null;
		
		public LogDataFromProduction(String prodString){
			try {
				HashMap<String,Object> result =
				        new ObjectMapper().readValue( prodString , HashMap.class);
				//"topTxnId":"3377de54-1896f-153e4cf7de6000"
				result.put("Dyn1", ranDomDynamic1);
				//replace topTxnId
				result.put("txnId", result.get("txnId")+UUID.randomUUID().toString()+Thread.currentThread().getName());
				result.remove("duration");
				//result.put("Dyn2", "1.2." + Math.random() + "");
				//result.put("Dyn3", "sessionId" + RandomStringUtils.randomAlphabetic(2));
				//result.put("messageTimeStamp",  Instant.now().getEpochSecond());
				m_result = result;
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		public String getString(){
			try {
				return mapper.writeValueAsString(m_result);
			} catch (JsonGenerationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return "{topTxnId=bb21cdc4-4a-153e2ddd87800ddd0, appVersion=1.3.75, humanTime=2016-04-05 05:05:17.681, msgType=INFO}";
		}
		
		String ranDomDynamic1 = UUID.randomUUID().toString();
		String ranDomDynamic2 = "1.2." + Math.random() + "";
		String ranDomDynamic3 = "sessionId" + RandomStringUtils.randomAlphabetic(2);


	}

	public static class LogData {

		@Override
		public String toString() {
			return "LogData [logid=" + logid + ", dataLog=" + dataLog + "]";
		}

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
		String dataLog = "Log Test data  " + logid + " " + RandomStringUtils.randomAlphabetic(100) + " "
				+ " test data check if the result is already being used; if it's not already taken, "
				+ "it's suitable as a unique identifiercheck if the result is already being used; "
				+ "if it's not already taken, it's suitable as a unique identifiercheck "
				+ "if the result is already being used; if it's not already taken,"
				+ " it's suitable as a unique identifiercheck " + "if the result is already being used; "
				+ "if it's not already taken, it's suitable as a "
				+ "unique identifiercheck if the result is already being used; "
				+ "if it's not already taken, it's suitable as a unique "
				+ "identifiercheck if the result is already being used; if "
				+ "it's not already taken, it's suitable as a unique identifiercheck if the result is already being used; if it's not already taken, it's suitable as a unique identifiercheck if the result is already being used; if it's not already taken, it's suitable as a unique identifier";

	}

	public void startTheLoadProcessUsingESNodeClient(IDataHolder config) {

	}

}


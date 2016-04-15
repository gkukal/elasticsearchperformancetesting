package com.jg.elasticsearch.load;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;

import com.google.common.base.Stopwatch;
import com.jg.elasticsearch.client.RestClient;
import com.jg.elasticsearch.load.reportmetrics.FetchESMetrics;
import com.jg.util.dataholder.GenericDataHolder;
import com.jg.util.dataholder.IDataHolder;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

public class LoadESWithSearchAndAggQueries {
	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(LoadESWithSearchAndAggQueries.class);

	public static void main(String[] args) {
		String fileName = null;
		String readFileToString = null;
		if (args != null && args.length > 0) {
			fileName = args[0];
			logger.info("Given Filepath for the config : " + fileName);
			try {
				readFileToString = FileUtils.readFileToString(new File(fileName));
				logger.info("Config file content : " + readFileToString);
			} catch (Exception e) {
				logger.error("Error while reading config file " + fileName, e);
			}

			IDataHolder config = new GenericDataHolder(readFileToString);
			logger.info(config != null ? config.getJSONData() : "NO config obj created. Returning ");
			if (config.getJSONRootNode() == null)
				return;
			try {
				// RestClient client = new RestClient();
				// JestClient tmpClient = client.createRestClient(config);
				// new
				// LoadESWithSearchAndAggQueries().getInitialData(tmpClient,"cerca_performance_march18",
				// "cerca_performance_march18", config);
				String log_level = config.getStringValue("loadconfig", new String[] { "load_infrastructure", "loglevel" });
				if( log_level == null) log_level = "info";
				if( log_level.equals("debug")){
					LogManager.getLogger("com.walmart.cerca").setLevel( Level.DEBUG);
				}else if( log_level.equals("info")) {
					LogManager.getLogger("com.walmart.cerca").setLevel( Level.INFO);
				}else if( log_level.equals("error")) {
					LogManager.getLogger("com.walmart.cerca").setLevel( Level.ERROR);
				}
				new LoadESWithSearchAndAggQueries().loadData(config);
			} catch (Exception e) {
				logger.error("Error while loading data to ES ", e);
			}

		} else {
			logger.error("No config provided so returning. Provide full file path as argument to Main class ");
			return;
		}
	}

	public static class LoadSearchTask implements Callable<SearchResult> {
		private static final Logger logger = org.slf4j.LoggerFactory.getLogger(LoadSearchTask.class);
		String m_indexName = null;
		String m_type = null;
		JestClient m_client = null;
		String m_query = null;

		private LoadSearchTask(JestClient client, String indexName, String type, String query) {
			m_indexName = indexName;
			m_type = type;
			m_query = query;
			m_client = client;
		}

		public SearchResult call() throws Exception {
			Search search = new Search.Builder(m_query)
					// multiple index or types can be added.
					.addIndex(m_indexName).addType(m_type).build();
			if( logger.isDebugEnabled() ) logger.debug(m_query);
			SearchResult result = m_client.execute(search);
			logger.debug( m_client.toString());

			if (logger.isDebugEnabled()) logger.debug(result.getJsonString());
			return result;
		}

	}

	public void loadData(IDataHolder config) {
		// Integer delay = config.getIntegerValue("loadconfig", new
		// String[]{"metricscapture","timeinterval_fetch_seconds"});
		Integer threads = config.getIntegerValue("loadconfig", new String[] { "load_infrastructure", "threads" });
		if (threads == null)
			threads = 1;
		Integer throttle_time_in_ms = config.getIntegerValue("loadconfig", new String[] { "load_infrastructure", "throttle_in_ms" });
		if( throttle_time_in_ms == null ) throttle_time_in_ms = 2000; //For bad users.
		
		Integer experimentTimeInSeconds = config.getIntegerValue("loadconfig",
				new String[] { "experiment", "run_for_seconds" });

		// querytemplate text_search_only template ##datalogvalue1##
		String jsonQuery = config.getJSONData("loadconfig",
				new String[] { "querytemplate", "text_search_only", "template" });

		final ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(threads);

		ScheduledExecutorService scheduledServiceInitialFetchCall = Executors.newScheduledThreadPool(1);
		FetchESMetrics.FetchMetricsTask task = new FetchESMetrics.FetchMetricsTask(config);
		scheduledServiceInitialFetchCall.scheduleWithFixedDelay(task, 0, 3, TimeUnit.MINUTES);

		// =====timer to stop shutdown everything at the end of time duration
		// ========================================================================
		Timer timerToStop = new Timer();
		timerToStop.schedule(new TimerTask() {
			@Override
			public void run() {
				newFixedThreadPool.shutdownNow();
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				logger.info("Shutting down all the threads. The duration of test is over.");
				System.exit(0);
			}
		}, experimentTimeInSeconds * 1000);
		// ========================================================================
		RestClient client = new RestClient();
		List<JestClient> restClientList = client.createMultipleRestClients(config);
		String indexName = config.getStringValue("loadconfig", new String[] { "elasticsearch", "indexName" });
		String typeName = config.getStringValue("loadconfig", new String[] { "elasticsearch", "typeNameOfIndex" });

		Stopwatch startTime = Stopwatch.createStarted();
		Map<String, List<String>> initialDataSet = this.getInitialData(restClientList.get(0), indexName, typeName,
				config);
		// List<String> listOfDataTextTokens = new ArrayList<String>();
		// listOfDataTextTokens.addAll(initialDataSet);

		Map<String, Integer> mapKeyToTokensUsed = new HashMap<String, Integer>();
		while (true) {
			
			if (startTime.elapsed(TimeUnit.MINUTES) > 1) {
				// call again to fetch data
				initialDataSet = this.getInitialData(restClientList.get(0), indexName, typeName, config);
				// listOfDataTextTokens.clear();
				// listOfDataTextTokens.addAll(initialDataSet);
				// reset watch
				startTime.reset();
				startTime.start();
			}

			List<Future<SearchResult>> listOfresults = new ArrayList<Future<SearchResult>>();
			int maxClients = restClientList.size();
			int iCntClient = 0;

			for (int i = 0; i < threads; i++) {
				String query = jsonQuery;
				for (String srckey : m_mapSourceKeyToMatchPattern.keySet()) {
					String patternToMatch = m_mapSourceKeyToMatchPattern.get(srckey);
					List<String> setOfTokens = initialDataSet.get(srckey);

					if (mapKeyToTokensUsed.get(srckey) == null) {
						mapKeyToTokensUsed.put(srckey, 0);
					}
					// int iCntTokens = mapKeyToTokensUsed.get(key) == null ? 0;
					int iCntTokens = mapKeyToTokensUsed.get(srckey);

					query = query.replaceAll(patternToMatch, setOfTokens.get(iCntTokens));

					if (mapKeyToTokensUsed.get(srckey) == setOfTokens.size() - 1) {
						mapKeyToTokensUsed.put(srckey, 0);
					} else {
						mapKeyToTokensUsed.put(srckey, ++iCntTokens);
					}
				}

				Future<SearchResult> resultFuture = newFixedThreadPool
						.submit(new LoadSearchTask(restClientList.get(iCntClient), indexName, typeName, query));
				listOfresults.add(resultFuture);
				if (iCntClient == (maxClients - 1)) {
					iCntClient = 0;
				} else {
					iCntClient++;
				}
			}
			for (Future<SearchResult> singleInstanceOfFutureObj : listOfresults) {
				try {
					SearchResult bulkResult = singleInstanceOfFutureObj.get();
					if (bulkResult == null) {
						// There was an issue
						logger.error("Issue in getting result from bulk calls");

					} else {

					}
				} catch (Exception e) {
					logger.error("Fatal error while making Bulk call to ES", e);
				}
			}
			try {
				logger.info("Throttling , sleeping time : "+throttle_time_in_ms);
				Thread.sleep( throttle_time_in_ms );
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//sleep
		
 	}

	private Map<String, String> m_mapSourceKeyToMatchPattern = new HashMap<String, String>();

	private void fetchSourceKeyToPattern(IDataHolder config) {
		List<String> keysToExtract = config.getChildKeys("loadconfig",
				new String[] { "querytemplate", "text_search_only", "textfields_initialquery" });
		if (keysToExtract != null) {
			for (String key : keysToExtract) {
				String matchString = config.getStringValue("loadconfig",
						new String[] { "querytemplate", "text_search_only", "textfields_initialquery", key, "match" });
				m_mapSourceKeyToMatchPattern.put(key, matchString);
			}
		}
	}

	public Map<String, List<String>> getInitialData(JestClient client, String indexName, String type,
			IDataHolder config) {
		String jsonDataInitialQuery = config.getJSONData("loadconfig",
				new String[] { "querytemplate", "initialquery" });

		Search search = new Search.Builder(jsonDataInitialQuery)
				// multiple index or types can be added.
				.addIndex(indexName).addIndex(type).build();

		Map<String, Set<String>> mapOfSetOfWords = new HashMap<String, Set<String>>();

		try {
			SearchResult result = client.execute(search);

			String jsonResult = result.getJsonString();
			GenericDataHolder jsonObj = new GenericDataHolder(null, jsonResult);
			List<IDataHolder> objectsAsDataHolderFromArray = jsonObj.getObjectsAsDataHolderFromArray(null,
					new String[] { "hits", "hits" });
			List<String> keysToExtract = config.getChildKeys("loadconfig",
					new String[] { "querytemplate", "text_search_only", "textfields_initialquery" });
			Map<String, String> mapSourceKeyToMatchPattern = new HashMap<String, String>();
			if (keysToExtract != null) {
				for (String key : keysToExtract) {
					String matchString = config.getStringValue("loadconfig", new String[] { "querytemplate",
							"text_search_only", "textfields_initialquery", key, "match" });
					mapSourceKeyToMatchPattern.put(key, matchString);
				}
			}
			m_mapSourceKeyToMatchPattern = mapSourceKeyToMatchPattern;
			// _source --> dataLog
			for (IDataHolder dataHolder : objectsAsDataHolderFromArray) {
				Set<String> keySet = mapSourceKeyToMatchPattern.keySet();
				if (keySet != null) {
					for (String key : keySet) {
						String textField = dataHolder.getStringValue(null, new String[] { "_source", key });
						String[] arrayOfWords = StringUtils.split(textField, " ");
						int cnt = 0;
						if( arrayOfWords == null ) continue;
						for (String str : arrayOfWords) {
							if( str.length() < 5 ) continue;
							if (cnt++ < 10) {
								Set<String> set = mapOfSetOfWords.get(key);
								if (set == null) {
									mapOfSetOfWords.put(key, new HashSet<String>());
								}
								if (mapOfSetOfWords.get(key).size() < 5000)
									mapOfSetOfWords.get(key).add(str);
							}
						}
					}
				}
			}
			// System.out.println("Hello "+setOfWords.size());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Map<String, List<String>> mapOfListOfWords = new HashMap<String, List<String>>();
		for (String key : mapOfSetOfWords.keySet()) {
			mapOfListOfWords.put(key, new ArrayList<String>(mapOfSetOfWords.get(key)));
		}
		return mapOfListOfWords;
	}

}

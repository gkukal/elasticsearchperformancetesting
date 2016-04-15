package com.jg.elasticsearch.load;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.google.common.base.Stopwatch;
import com.jg.elasticsearch.load.reportmetrics.FetchESMetrics;
import com.jg.elasticsearch.load.reportmetrics.MetricHelper;
import com.jg.util.dataholder.GenericDataHolder;
import com.jg.util.dataholder.IDataHolder;

public class MetricFetcherFromESAndPushToGraphite {

	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(LoadDataToElasticSearch.class);

	public void metricsCreationThread(IDataHolder config) {

		Integer delay = config.getIntegerValue("loadconfig", new String[] { "metricscapture", "timeinterval_fetch_seconds" });
		if (delay == null)
			delay = 60;
		ScheduledExecutorService scheduledService = Executors.newScheduledThreadPool(1);
		FetchESMetrics.FetchMetricsTask task = 
				new FetchESMetrics.FetchMetricsTask(config);
		scheduledService.scheduleWithFixedDelay(task, 0, 60, TimeUnit.SECONDS);
		Stopwatch startTime = Stopwatch.createStarted();
		while (startTime.elapsed(TimeUnit.SECONDS) < 60*60) {
			
		}
		System.exit(0);
	}
	
	public static void main(String[] args ){
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
				MetricHelper.initialize(config);
				new MetricFetcherFromESAndPushToGraphite().metricsCreationThread(config);
			} catch (Exception e) {
				logger.error("Error while loading data to ES ", e);
			}

		} else {
			logger.error("No config provided so returning. Provide full file path as argument to Main class ");
			return;
		}
	}
}

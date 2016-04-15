package com.jg.elasticsearch.load.reportmetrics;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.jg.util.dataholder.IDataHolder;

public class MetricHelper {

	private static MetricHelper helper = null;
	private MetricRegistry metrics = new MetricRegistry();
	private String m_nameSpace = null;
	
	public synchronized static MetricHelper initialize(IDataHolder config) {

		if (helper == null) {
			helper = new MetricHelper();
			String experimentName = config.getStringValue("loadconfig", new String[] { "experiment", "name" });
			helper.m_nameSpace = experimentName;
			// Start the reporter
//			ConsoleReporter reporter = ConsoleReporter.forRegistry(helper.metrics).convertRatesTo(TimeUnit.SECONDS)
//					.convertDurationsTo(TimeUnit.MILLISECONDS).build();
//			reporter.start(1, TimeUnit.MINUTES);

//			Slf4jReporter reporterLog4J = Slf4jReporter.forRegistry(helper.metrics)
//					.outputTo(LoggerFactory.getLogger("com.example.metrics")).convertRatesTo(TimeUnit.SECONDS)
//					.convertDurationsTo(TimeUnit.MILLISECONDS).build();
//			reporterLog4J.start(1, TimeUnit.MINUTES);

			try {
				try {
					String csvFolder= config.getStringValue( "loadconfig" , new String[] { "metricscapture", "metricssink", "csvfolder" });
					File folderCVS =  new File( csvFolder+"/"+experimentName+"_"+System.currentTimeMillis());
					FileUtils.forceMkdir( folderCVS );
					final CsvReporter reporterCSV = CsvReporter.forRegistry(helper.metrics)
					        .formatFor(Locale.US)
					        .convertRatesTo(TimeUnit.SECONDS)
					        .convertDurationsTo(TimeUnit.MILLISECONDS)
					        .build( folderCVS );
					reporterCSV.start(1, TimeUnit.MINUTES);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// Add graphite reporter
			String graphiteServer = config.getStringValue("loadconfig",
					new String[] { "metricscapture", "metricssink", "graphite", "server" });
			Integer graphitePort = config.getIntegerValue("loadconfig",
					new String[] { "metricscapture", "metricssink", "graphite", "port" });
			if (graphiteServer != null) {
				Graphite graphite = new Graphite(new InetSocketAddress(graphiteServer, graphitePort));
				com.codahale.metrics.graphite.GraphiteReporter reporterGraphite = 
						com.codahale.metrics.graphite.GraphiteReporter
						.forRegistry(helper.metrics)
						.convertDurationsTo(TimeUnit.SECONDS)
						.convertDurationsTo(TimeUnit.MILLISECONDS)
						.filter(MetricFilter.ALL)
						.build(graphite)
						;
				
				
				reporterGraphite.start(2, TimeUnit.MINUTES);
				
			}
		}
		
		return helper;
	}

	public static MetricHelper metricRegistry() {
		if (helper == null)
			throw new RuntimeException("MetricHelper initialize method needs to be called first");
		return helper;
	}

	public String getNamespace() {
		return m_nameSpace;
	}

	public void addToGauge(Long value, String... names) {
		if (value == null)
			value = 1L;
		Meter meterObj = metrics.meter(MetricRegistry.name(m_nameSpace, names));
		meterObj.mark(value);
	}

	public void addToMeter(Long value, String... names) {
		if (value == null)
			value = 1L;
		Meter meterObj = metrics.meter(MetricRegistry.name(m_nameSpace, names));
		meterObj.mark(value);
	}

	public void addToCounter(Long value, String... names) {
		if (value == null)
			value = 1L;
		Counter counterObj = metrics.counter(MetricRegistry.name(m_nameSpace, names));
		counterObj.inc();

	}

	public Timer.Context startTimer(String... names) {
		Timer timerObj = metrics.timer(MetricRegistry.name(m_nameSpace, names));
		Timer.Context context = timerObj.time();
		return context;
	}

	public Boolean stopTimer(Timer.Context timerContext) {
		return timerContext.stop() >= 0;
	}
	
	public static class CustomMetricsFilter implements MetricFilter{

		public boolean matches(String name, Metric metric) {
			// TODO Auto-generated method stub
			if( name == null || name.endsWith("_rate")) return false;
			return true;
		}
		
	}

}

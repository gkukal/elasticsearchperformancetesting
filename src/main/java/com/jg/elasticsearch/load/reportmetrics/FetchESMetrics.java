package com.jg.elasticsearch.load.reportmetrics;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import com.jg.elasticsearch.util.JsonFlattener;
import com.jg.util.dataholder.DataTypeEnum;
import com.jg.util.dataholder.GenericDataHolder;
import com.jg.util.dataholder.IDataHolder;

public class FetchESMetrics {

	public static void main(String[] args ){
		

		File file = new File( FetchESMetrics.class.getClassLoader().getResource("loadconfig.config").getFile() );
		String jsonData = null;
		
		try {
			jsonData = FileUtils.readFileToString(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		IDataHolder config = new GenericDataHolder(jsonData);
		MetricHelper.initialize(config);
		ScheduledExecutorService scheduler =
			     Executors.newScheduledThreadPool(1);
		scheduler.scheduleAtFixedRate( new FetchMetricsTask( config ), 0, 20, TimeUnit.SECONDS);
		
	}
	
	
	public IDataHolder getMetrics(IDataHolder configObj ){
		 return null;
	}

	public static class FetchMetricsTask implements Runnable {

		IDataHolder m_config = null;
		
		public FetchMetricsTask(IDataHolder config ){
			m_config = config ;
		}
		
		public void run() {
			String clusterName = 
					m_config.getStringValue("loadconfig", new String[]{"elasticsearch","cluster.name"});
			List<String> ipList = 
					m_config.getStringValues("loadconfig", new String[]{"elasticsearch","cluster_host_ips"});
			String graphiteServer = 
					m_config.getStringValue("loadconfig", new String[]{"metricscapture","metricssink","graphite","server"});
			String graphiteServerPort = 
					m_config.getStringValue("loadconfig", new String[]{"metricscapture","metricssink","graphite","port"});
			
			
			// get the node stats...
			// Make call http://100.64.10.142:9200/_nodes/stats/
			String dataJson = HttpUtil.get("http://"+ipList.get(0)+":9200//_nodes/stats/");
			if( dataJson != null ){
			      String flatJson = JsonFlattener.flatten( new String( dataJson ) );
			      System.out.println( flatJson );
			      IDataHolder metricsFlatObjJson =  new GenericDataHolder( null , flatJson);
			      IDataHolder metricsObjJson =  new GenericDataHolder( null , dataJson);
			      List<String> childKeys = metricsObjJson.getChildKeys(null, "nodes");
			      if( childKeys != null ){
			    	  for( String key : childKeys ){
			    		//get the IP/Nodename 
			    		 String nodeName =  metricsFlatObjJson.getStringValue(null, "nodes."+key+".transport_address");
			    		 nodeName = nodeName.replaceAll("\\.", "_");
			    		 for( String singleMetricFromList : metricsNodes ){
			    			 DataTypeEnum type = metricsFlatObjJson.getType( null, new String[]{"nodes."+key+"."+ singleMetricFromList} );
			    			 if( type == null ) continue;
			    			 if( type == DataTypeEnum.NUMERIC_INTEGER){
			    				 Integer intValue = metricsFlatObjJson.getIntegerValue( null, "nodes."+key+"."+ singleMetricFromList );
			    				 MetricHelper.metricRegistry().addToMeter( new Long(intValue), new String[]{nodeName,singleMetricFromList} );
			    			 }else if( type == DataTypeEnum.NUMERIC_LONG){
			    				 Long longValue = metricsFlatObjJson.getLongValue( null, "nodes."+key+"."+ singleMetricFromList );
			    				 MetricHelper.metricRegistry().addToMeter( new Long(longValue), new String[]{nodeName,singleMetricFromList} );
			    			 }
			    		 }
			    	  }
			      }
			}
			//Get two json strings original and one which is flat.
			// get the node.<nodeid> by child of key nodes
			//concatenate to the preconfigured metrics and get it from flat structue.
			//-- before we do that we need to know the data type of each node
			//once we know.. we use that to put the IP and put in metric in propert format...
			//and codahale is generated..
		}

	}
	
	
	public static final String[] metricsNodes = new String[]
			{
					"indices.docs.count",
					"indices.store.size_in_bytes",
					"indices.store.throttle_time_in_millis",
					"indices.indexing.index_total",
					"indices.indexing.index_time_in_millis",
					"indices.indexing.index_failed",
					"indices.indexing.throttle_time_in_millis",
					"indices.merges.total"

			};
			}


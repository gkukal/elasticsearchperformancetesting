package com.jg.elasticsearch.load;

import static org.elasticsearch.node.NodeBuilder.*;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.threadpool.ThreadPoolStats;

public class LoadRandomLogDataToES {

	public static void main(String[] args) {
		
//		CommandLineParser parser = new DefaultParser();
//		Options options = new Options();
//
//		// add t option
//		options.addOption("nodetype", false, "Defines the nodetype to be used. Includes REST or NODECLIENT" );
//		CommandLine cmd = null;
//		try {
//			cmd = parser.parse( options, args);
//			if(cmd.hasOption("nodetype")) {
//			    // print the date and time
//			}
//			else {
//			    // print the date
//			}
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		String loggerLevel = System.getProperty("es.logger.level", "DEBUG");
        Environment env = InternalSettingsPreparer.prepareEnvironment(Settings.builder()
                .put("appender.console.type", "console")
                .put("appender.console.layout.type", "consolePattern")
                .put("appender.console.layout.conversionPattern","[%d{ISO8601}][%-5p][%-25c] %m%n")
                .put("rootLogger", "${es.logger.level}, console")
                .put("es.logger.level", loggerLevel)
                .build(), Terminal.DEFAULT);
        // configure but do not read the logging conf file
        LogConfigurator.configure(env.settings(), false);
		
		NodeBuilder nodeBldr = nodeBuilder().
				clusterName("elasticsearch")
				
				.settings( 
						Settings.settingsBuilder()
						.put("http.enabled", true)
						.put("node.name","JGTestclient5")
						.put("discovery.zen.ping.multicast.enabled",false)
						.put("discovery.zen.ping.multicast.enabled",false)
						.put("discovery.zen.ping.unicast.enabled", true) 
						//.put("network.host", "0.0.0.0")
						.put("network.bind_host", "0.0.0.0")
						//.put("network.publish_host", "_non_loopback_")

						//.put("transport.tcp.port", "9300") 
						.put("node.data", false) 
						.put("node.master", false) 
						
						.putArray("discovery.zen.ping.unicast.hosts",
								"0.0.0.0"
								)
						)
				.client(true);
		System.out.println("Hello world ");
		Node clientNode =  nodeBldr.node();
		//clientNode = clientNode.start();
		System.out.println("node"+clientNode.toString());
		clientNode = clientNode.start();
		Client client = clientNode.client();
		try {
			System.out.println("Sleeping"+clientNode.toString());
			 ClusterStatsResponse clusterStatsResponse = client.admin().cluster().clusterStats(new ClusterStatsRequest()).actionGet();
			System.out.println("INFO : "+ clusterStatsResponse.getIndicesStats().getIndexCount() );
		    NodesStatsResponse response = client.admin().cluster().prepareNodesStats().setThreadPool(true).execute().actionGet();
		    NodeStats[] nodeStats2 = response.getNodes();

		    for (NodeStats nodeStats3 : nodeStats2) {
		        ThreadPoolStats stats = nodeStats3.getThreadPool();
		        if (stats != null)
		            for (ThreadPoolStats.Stats threadPoolStat : stats) {
		                System.out.println("node `" + nodeStats3.getNode().getName() + "`" + " has pool `" + threadPoolStat.getName() + "` with current queue size " + threadPoolStat.getQueue());
		            }
		    }
			Thread.sleep(300000);
		} catch ( Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("closing"+clientNode.toString());
		clientNode.close();
	}
}

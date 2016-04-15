package com.jg.elasticsearch.client;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.slf4j.Logger;

import com.google.common.base.Stopwatch;
import com.jg.elasticsearch.load.StartADummyClientNode;
import com.jg.util.dataholder.IDataHolder;

/**
 * 
 *
 */
public class ESNodeClientDaemon {

	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ESNodeClientDaemon.class);
	public void keepClientNodeUpForGivenDuration(IDataHolder config) {
		Long durationToKeepNodeUpInMinutes = config.getLongValue("esnodeclient", "duration_in_minutes");
		if( durationToKeepNodeUpInMinutes == null ) durationToKeepNodeUpInMinutes = 10L;
		//Stopwatch watchStart = Stopwatch.createStarted();
		Client node = startNode(config);
		try {
			Thread.sleep(durationToKeepNodeUpInMinutes*60*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		stopNode(node);
	}

	public Client startNode(IDataHolder config) {
		String loglevel = config.getStringValue("esnodeclient", "loglevel");
		String loggerLevel = System.getProperty("es.logger.level", loglevel);
		Environment env = InternalSettingsPreparer.prepareEnvironment(
				Settings.builder().put("appender.console.type", "console")
						.put("appender.console.layout.type", "consolePattern")
						.put("appender.console.layout.conversionPattern", "[%d{ISO8601}][%-5p][%-25c] %m%n")
						.put("rootLogger", "${es.logger.level}, console").put("es.logger.level", loggerLevel).build(),
				Terminal.DEFAULT);
		// configure but do not read the logging conf file
		LogConfigurator.configure(env.settings(), false);

		String clusterName = config.getStringValue("esnodeclient", "cluster.name");
		String nodeName = config.getStringValue("esnodeclient", "nodename");
		List<String> ipFromConfig = config.getStringValues("esnodeclient", "cluster_host_ips");
		
		String network_publish_host = config.getStringValue("esnodeclient", "network.publish_host");
		String network_host = config.getStringValue("esnodeclient", "network.host");
		String transport_tcp_port = config.getStringValue("esnodeclient", "transport.tcp.port");
		String network_bind_host = config.getStringValue("esnodeclient", "network.bind_host");
		
		
		String[] ipsArr = new String[ipFromConfig.size()];
		ipsArr = ipFromConfig.toArray(ipsArr);

		
		Builder builder = Settings.settingsBuilder().put("http.enabled", true).put("node.name", nodeName )
		.put("discovery.zen.ping.multicast.enabled", false)
		.put("discovery.zen.ping.unicast.enabled", true)
		.putArray("discovery.zen.ping.unicast.hosts",
				ipsArr)
		.put("node.data", false).put("node.master", false);
		
		

		if( network_publish_host != null && !network_publish_host.isEmpty()) builder.put("network.publish_host", network_publish_host);
		if( network_host != null  && !network_host.isEmpty() ) builder.put("network.host", network_host);
		if( transport_tcp_port != null && !transport_tcp_port.isEmpty()) builder.put("transport.tcp.port", transport_tcp_port);
		if( network_bind_host != null && !network_bind_host.isEmpty()) builder.put("network.bind_host", network_bind_host);
		
		// .put("network.host", "0.0.0.0")
		//.put("network.bind_host", "0.0.0.0")
		// .put("network.publish_host", "_non_loopback_")
		// .put("transport.tcp.port", "9300")
		// .put("network.publish_host", "0.0.0.0")
		
		NodeBuilder nodeBldr = nodeBuilder().clusterName(clusterName)
				.settings(builder)
				.client(true);
		Node clientNode = nodeBldr.node();

		// clientNode = clientNode.start();
		logger.info("node" + clientNode.toString());
		clientNode = clientNode.start();
		Client client = clientNode.client();

		return client;
	}

	public void stopNode(Client nodeToBeStopped) {
		logger.info("node stopping " + nodeToBeStopped.toString());
		nodeToBeStopped.close();
	}

}

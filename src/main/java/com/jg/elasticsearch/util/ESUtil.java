package com.jg.elasticsearch.util;

import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.math.MathUtils;
import org.slf4j.Logger;

import com.jg.util.dataholder.GenericDataHolder;
import com.jg.util.dataholder.IDataHolder;

public class ESUtil {
	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ESUtil.class);

	public static String routingValueForThisShardId( String shardID , int numberOfShards){
		Integer shard = new Integer(shardID);
		Murmur3HashFunction function = new Murmur3HashFunction();
		for( int i= 0 ; i < 100000; i++ ){
			String haskKey = "elastic"+i+"";
			int hashId =  function.hash( haskKey );
			//System.out.println( "for i="+i+ " hash "+ hashId +" Bucket "+ MathUtils.mod( hashId , 100));
			int bucketId = MathUtils.mod( hashId , numberOfShards) ;
			if( bucketId == shard ) {
				logger.info("Found the routing key : for shardId :"+ shardID +" as "+haskKey + ", bucketId: "+bucketId);
				return haskKey;
			}
		}
		return null;
		
	}
	
	public static String getShardIDForThisIPAndIndex(String ip, String port, String index) {
		HttpClient client = new HttpClient();
		HttpMethod method = new GetMethod("http://" + ip + ":" + port + "/_cluster/state");
		try {
			int statusCode = client.executeMethod(method);

			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			//System.out.println(new String(responseBody));
			String flatJson = JsonFlattener.flatten(new String(responseBody));
			GenericDataHolder metricsObjJson = new GenericDataHolder(null, new String(responseBody));

			List<String> nodeNames = metricsObjJson.getChildKeys(null, new String[] { "nodes" });
			String input_nodeIPToFind = ip;
			String input_indexName = index;
			String returnNodeName = null;
			if (nodeNames != null) {
				for (String nodeName : nodeNames) {
					String ipaddress = metricsObjJson.getStringValue(null,
							new String[] { "nodes", nodeName, "transport_address" });
					if (ipaddress != null) {
						ipaddress = StringUtils.substringBefore(ipaddress, ":");
						if (input_nodeIPToFind.equals(ipaddress)) {
							returnNodeName = nodeName;
						}
					}
				}
			}

			List<IDataHolder> objectsAsDataHolderFromArray = metricsObjJson.getObjectsAsDataHolderFromArray(null,
					new String[] { "routing_nodes", "nodes", returnNodeName });
			String returnShardID = null;
			if (objectsAsDataHolderFromArray != null) {
				for (IDataHolder holder : objectsAsDataHolderFromArray) {
					String shardId = holder.getStringValue(null, new String[] { "shard" });
					Boolean primary = holder.getBooleanValue(null, new String[] { "primary" });
					String indexName = holder.getStringValue(null, new String[] { "index" });
					if (primary != null && primary && indexName.equals(input_indexName)) {
						returnShardID = shardId;
					}
				}
			}
			return returnShardID;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}

package com.jg.elasticsearch.load;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.jg.elasticsearch.client.ESNodeClientDaemon;
import com.jg.util.dataholder.GenericDataHolder;
import com.jg.util.dataholder.IDataHolder;

public class StartADummyClientNode {

	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(StartADummyClientNode.class);

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
			ESNodeClientDaemon daemon = new ESNodeClientDaemon();
			daemon.keepClientNodeUpForGivenDuration(config );
			//fileName = 
		}else{
			return;
		}
	}
}

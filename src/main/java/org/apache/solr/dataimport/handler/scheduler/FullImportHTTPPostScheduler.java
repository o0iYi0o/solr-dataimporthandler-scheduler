package org.apache.solr.dataimport.handler.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;

public class FullImportHTTPPostScheduler extends BaseTimerTask {
    private static final Logger logger = LoggerFactory.getLogger(FullImportHTTPPostScheduler.class);

    public FullImportHTTPPostScheduler(String webAppName, Timer t) throws Exception {
        super(webAppName, t);
        logger.info("<index update process> DeltaImportHTTPPostScheduler init");
    }

    public void run() {
    	logger.info("************************* solr 全量更新  ******************");
        try {
            if ((this.server.length() == 0) || (this.webapp.length() == 0) ||
                    (this.reBuildIndexParams == null) ||
                    (this.reBuildIndexParams.length() == 0)) {
                logger.warn("<index update process> Insuficient info provided for data import, reBuildIndexParams is null");
                logger.info("<index update process> Reloading global dataimport.properties");
                reloadParams();
            } else if (this.singleCore) {
                prepUrlSendHttpPost(this.reBuildIndexParams);
            } else if ((this.syncCores.length == 0) || (
                    (this.syncCores.length == 1) && (this.syncCores[0].length() == 0))) {
                logger.warn("<index update process> No cores scheduled for data import");
                logger.info("<index update process> Reloading global dataimport.properties");
                reloadParams();
            } else {
                for (String core : this.syncCores)
                    prepUrlSendHttpPost(core, this.reBuildIndexParams);
            }
        } catch (Exception e) {
            logger.error("Failed to prepare for sendHttpPost", e);
            reloadParams();
        }
    }
}
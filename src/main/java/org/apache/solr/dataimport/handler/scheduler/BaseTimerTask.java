package org.apache.solr.dataimport.handler.scheduler;

import com.sun.org.apache.xml.internal.security.utils.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public abstract class BaseTimerTask extends TimerTask {
    protected String syncEnabled;
    protected String[] syncCores;
    protected String server;
    protected String port;
    protected String webapp;
    protected String params;
    protected String interval;
    protected String cores;
    protected SolrDataImportProperties p;
    protected boolean singleCore;
    protected String reBuildIndexParams;
    protected String reBuildIndexBeginTime;
    protected String reBuildIndexInterval;
    protected String authorization;
    protected static final Logger logger = LoggerFactory.getLogger(BaseTimerTask.class);

    public BaseTimerTask(String webAppName, Timer t) throws Exception {
        this.p = new SolrDataImportProperties();
        reloadParams();
        fixParams(webAppName);

        if (!this.syncEnabled.equals("1")) {
            throw new Exception("Schedule disabled");
        }
        if ((this.syncCores == null) || ((this.syncCores.length == 1) && (this.syncCores[0].length() == 0))) {
            this.singleCore = true;
            logger.info("<index update process> Single core identified in dataimport.properties");
        } else {
            this.singleCore = false;
            logger.info("<index update process> Multiple cores identified in dataimport.properties. Sync active for: " +
                    this.cores);
        }
    }

    protected void reloadParams() {
        this.p.loadProperties(true);
        this.syncEnabled = this.p.getProperty(SolrDataImportProperties.SYNC_ENABLED);
        this.cores = this.p.getProperty(SolrDataImportProperties.SYNC_CORES);
        this.server = this.p.getProperty(SolrDataImportProperties.SERVER);
        this.port = this.p.getProperty(SolrDataImportProperties.PORT);
        this.webapp = this.p.getProperty(SolrDataImportProperties.WEBAPP);
        this.params = this.p.getProperty(SolrDataImportProperties.PARAMS);
        this.interval = this.p.getProperty(SolrDataImportProperties.INTERVAL);
        this.syncCores = (this.cores != null ? this.cores.split(",") : null);

        this.reBuildIndexParams = this.p
                .getProperty(SolrDataImportProperties.REBUILDINDEXPARAMS);
        this.reBuildIndexBeginTime = this.p
                .getProperty(SolrDataImportProperties.REBUILDINDEXBEGINTIME);
        this.reBuildIndexInterval = this.p
                .getProperty(SolrDataImportProperties.REBUILDINDEXINTERVAL);
        this.authorization = this.p.getProperty(SolrDataImportProperties.AUTHORIZATION);
    }

    protected void fixParams(String webAppName) {
        if ((this.server == null) || (this.server.length() == 0))
            this.server = "localhost";
        if ((this.port == null) || (this.port.length() == 0))
            this.port = "8080";
        if ((this.webapp == null) || (this.webapp.length() == 0))
            this.webapp = webAppName;
        if ((this.interval == null) || (this.interval.length() == 0) || (getIntervalInt() <= 0)) {
            this.interval = "30";
        }
        if (reBuildIndexBeginTime == null || reBuildIndexBeginTime.length() == 0)
            interval = "00:00:00";
        if ((this.reBuildIndexInterval == null) || (this.reBuildIndexInterval.length() == 0) ||
                (getReBuildIndexIntervalInt() <= 0))
            this.reBuildIndexInterval = "0";
    }
    protected void prepUrlSendHttpPost(String params) {
        String coreUrl = "http://" + this.server + ":" + this.port + "/" + this.webapp + params;
        sendHttpPost(coreUrl, null);
    }

    protected void prepUrlSendHttpPost(String coreName, String params) {
        String coreUrl = "http://" + this.server + ":" + this.port + "/" + this.webapp + "/" + coreName + params;
        sendHttpPost(coreUrl, coreName);
    }

    protected void sendHttpPost(String completeUrl, String coreName) {
        DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss SSS");
        Date startTime = new Date();

        //String core = "[" + coreName + "] ";
        String core = coreName == null ? "" : "[" + coreName + "] ";
        logger.info(core +
                "<index update process> Process started at .............. " +
                df.format(startTime));
        try {
            URL url = new URL(completeUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("type", "submit");
            conn.setDoOutput(true);
            if (this.authorization != null){
                String encoding = Base64.encode(this.authorization.getBytes());
                conn.setRequestProperty  ("Authorization", "Basic " + encoding);
            }
            conn.connect();

            logger.info(core + "<index update process> Full URL\t\t\t\t" +
                    conn.getURL());
            logger.info(core + "<index update process> Response message\t\t\t" +
                    conn.getResponseMessage());
            logger.info(core + "<index update process> Response code\t\t\t" +
                    conn.getResponseCode());

            if (conn.getResponseCode() != 200) {
                reloadParams();
            }

            conn.disconnect();
            logger.info(core +
                    "<index update process> Disconnected from server\t\t" +
                    this.server);
            Date endTime = new Date();
            logger.info(core +
                    "<index update process> Process ended at ................ " +
                    df.format(endTime));
        } catch (MalformedURLException mue) {
            logger.error("Failed to assemble URL for HTTP POST", mue);
        } catch (IOException ioe) {
            logger.error(
                    "Failed to connect to the specified URL while trying to send HTTP POST",
                    ioe);
        } catch (Exception e) {
            logger.error("Failed to send HTTP POST", e);
        }
    }

    public int getIntervalInt() {
        try {
            return Integer.parseInt(this.interval);
        } catch (NumberFormatException e) {
            logger.warn(
                    "Unable to convert 'interval' to number. Using default value (30) instead",
                    e);
        }
        return 30;
    }

    public int getReBuildIndexIntervalInt() {
        try {
            return Integer.parseInt(this.reBuildIndexInterval);
        } catch (NumberFormatException e) {
            logger.info(
                    "Unable to convert 'reBuildIndexInterval' to number. do't rebuild index.",
                    e);
        }
        return 0;
    }

    public Date getReBuildIndexBeginTime() {
        Date beginDate = null;
        try {
            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateStr = sdfDate.format(new Date());
            beginDate = sdfDate.parse(dateStr);
            if ((this.reBuildIndexBeginTime == null) ||
                    (this.reBuildIndexBeginTime.length() == 0))
                return beginDate;
            SimpleDateFormat sdf = null;
            if (this.reBuildIndexBeginTime.matches("\\d{2}:\\d{2}:\\d{2}")) {
                sdf = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
                beginDate = sdf.parse(dateStr + " " + this.reBuildIndexBeginTime);
            } else if (this.reBuildIndexBeginTime
                    .matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                sdf = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
            }
            return sdf.parse(this.reBuildIndexBeginTime);
        } catch (ParseException e) {
            logger.warn(
                    "Unable to convert 'reBuildIndexBeginTime' to date. use now time.",
                    e);
        }
        return beginDate;
    }
}
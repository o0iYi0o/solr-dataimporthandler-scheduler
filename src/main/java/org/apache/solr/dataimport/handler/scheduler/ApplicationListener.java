package org.apache.solr.dataimport.handler.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

public class ApplicationListener implements ServletContextListener {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationListener.class);


    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        ServletContext servletContext = servletContextEvent.getServletContext();

        Timer timer = (Timer) servletContext.getAttribute("timer");
        Timer fullImportTimer = (Timer) servletContext.getAttribute("fullImportTimer");

        if (timer != null)
            timer.cancel();
        if (fullImportTimer != null) {
            fullImportTimer.cancel();
        }

        servletContext.removeAttribute("timer");
        servletContext.removeAttribute("fullImportTimer");
    }

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        ServletContext servletContext = servletContextEvent.getServletContext();
        try {
            Timer timer = new Timer();
            DeltaImportHTTPPostScheduler task = new DeltaImportHTTPPostScheduler(
                    servletContext.getServletContextName(), timer);

            int interval = task.getIntervalInt();

            Calendar calendar = Calendar.getInstance();

            calendar.add(Calendar.MINUTE, interval);
            Date startTime = calendar.getTime();

            timer.scheduleAtFixedRate(task, startTime, 60000 * interval);

            servletContext.setAttribute("timer", timer);

            Timer fullImportTimer = new Timer();
            FullImportHTTPPostScheduler fullImportTask = new FullImportHTTPPostScheduler(
                    servletContext.getServletContextName(), fullImportTimer);

            int reBuildIndexInterval = fullImportTask
                    .getReBuildIndexIntervalInt();
            if (reBuildIndexInterval <= 0) {
                logger.warn("Full Import Schedule disabled");
                return;
            }

            Calendar fullImportCalendar = Calendar.getInstance();
            Date beginDate = fullImportTask.getReBuildIndexBeginTime();
            fullImportCalendar.setTime(beginDate);
            fullImportCalendar.add(Calendar.MINUTE, reBuildIndexInterval);
            Date fullImportStartTime = fullImportCalendar.getTime();

            fullImportTimer.scheduleAtFixedRate(fullImportTask,
                    fullImportStartTime, 60000 * reBuildIndexInterval);

            servletContext.setAttribute("fullImportTimer", fullImportTimer);
        } catch (Exception e) {
            if (e.getMessage().endsWith("disabled"))
                logger.warn("Schedule disabled");
            else
                logger.error("Problem initializing the scheduled task: ", e);
        }
    }
}
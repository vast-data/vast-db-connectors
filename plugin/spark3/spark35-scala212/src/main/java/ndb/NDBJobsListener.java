/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastAutocommitTransaction;
import com.vastdata.client.tx.ka.JobEventService;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static java.lang.String.format;

public class NDBJobsListener
        extends SparkListener
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBJobsListener.class);
    private static NDBJobsListener NDB_JOBS_LISTENER = null;
    private final JobEventService eventConsumer;
    private NDBJobsListener(JobEventService eventConsumer)
    {
        this.eventConsumer = eventConsumer;
    }

    public static synchronized SparkListener instance(Supplier<VastClient> vastClientSupplier, VastConfig vastConf)
    {
        if (NDB_JOBS_LISTENER == null) {
            LOG.debug("instance() - new");
            NDB_JOBS_LISTENER = new NDBJobsListener(JobEventService.createInstance(vastClientSupplier, vastConf));
        }
        else {
            LOG.debug("instance() - existing");
        }
        return NDB_JOBS_LISTENER;
    }


    @Override
    public void onJobStart(SparkListenerJobStart jobStart)
    {
        LOG.info("onJobStart() {}", format("SparkListenerJobStart: id:%s, time:%s", jobStart.jobId(), jobStart.time()));
        SimpleVastTransaction existing = VastAutocommitTransaction.getExisting();
        if (existing != null) {
            eventConsumer.notifyTxActivityStart(existing);
        }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd)
    {
        LOG.info("onJobEnd() {}", format("SparkListenerJobEnd: id:%s, time:%s, result:%s", jobEnd.jobId(), jobEnd.time(), jobEnd.jobResult()));
        SimpleVastTransaction existing = VastAutocommitTransaction.getExisting();
        if (existing != null) {
            eventConsumer.notifyTxActivityEnd(existing);
        }
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event)
    {
        if (event instanceof SparkListenerSQLExecutionStart) {
            LOG.info("onOtherEvent() - SQL execution start. id: {}", ((SparkListenerSQLExecutionStart) event).executionId());
        }
        else if (event instanceof SparkListenerSQLExecutionEnd) {
            long id = ((SparkListenerSQLExecutionEnd) event).executionId();
            LOG.info("onOtherEvent() - SQL execution end. id: {}", id);
            NDB.QUERY_TRANSACTIONS_REGISTRY.closeTransactions(id);
        } else {
            LOG.debug("onOtherEvent() unsupported event type: {}", event);
            super.onOtherEvent(event);
        }
    }
}

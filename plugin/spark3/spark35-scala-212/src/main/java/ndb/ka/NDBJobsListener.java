/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb.ka;

import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastIOException;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.ka.JobEventService;
import com.vastdata.spark.tx.VastAutocommitTransaction;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
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

    public static synchronized SparkListenerInterface instance(Supplier<VastClient> vastClientSupplier, VastConfig vastConf)
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
        try {
            SimpleVastTransaction existing = VastAutocommitTransaction.getExisting();
            if (existing != null) {
                eventConsumer.notifyTxActivityStart(existing);
            }
        }
        catch (VastIOException e) {
            LOG.error("Failed getting existing transaction", e);
        }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd)
    {
        LOG.info("onJobEnd() {}", format("SparkListenerJobEnd: id:%s, time:%s, result:%s", jobEnd.jobId(), jobEnd.time(), jobEnd.jobResult()));
        try {
            SimpleVastTransaction existing = VastAutocommitTransaction.getExisting();
            if (existing != null) {
                eventConsumer.notifyTxActivityEnd(existing);
            }
        }
        catch (VastIOException e) {
            LOG.error("Failed getting existing transaction", e);
        }
    }
}

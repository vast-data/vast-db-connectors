/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.execution.SparkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Unit;

public class NDBSparkSessionExtension
        implements Function1<SparkSessionExtensions, Unit>
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBSparkSessionExtension.class);
    public NDBSparkSessionExtension()
    {
//        LOG.debug("NDBSparkSessionExtension(): Trying to override spark table-scan classes");
//        try {
//            SparkClassOverrider.overrideV2Relation();
//        }
//        catch (NotFoundException | CannotCompileException e) {
//            LOG.error("Override spark table-scan classes failed during NDB extension init", e);
//        }
    }
    private static final Function1<SparkSession, SparkStrategy> STRATEGY_INJECTOR =
            session -> new NDBStrategy();

    @Override
    public Unit apply(SparkSessionExtensions sparkSessionExtensions)
    {
        LOG.debug("apply(): Trying to override spark table-scan classes");
//        try {
//            SparkClassOverrider.overrideV2Relation();
//        }
//        catch (NotFoundException | CannotCompileException e) {
//            LOG.error("Override spark table-scan classes failed during NDB extension injection", e);
//            throw new RuntimeException(e);
//        }
        sparkSessionExtensions.injectPlannerStrategy(STRATEGY_INJECTOR);
//        sparkSessionExtensions.injectQueryStagePrepRule(AQE_INJECTOR);
        return null;
    }
}

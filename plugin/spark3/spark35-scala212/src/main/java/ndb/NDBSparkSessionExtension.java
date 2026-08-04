/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.VastConfig;
import ndb.view.NDBTablesResolutionRule;
import org.apache.spark.SparkContext$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.execution.SparkStrategy;
import scala.Function1;
import scala.Unit;
import scala.collection.immutable.Map;

public class NDBSparkSessionExtension
        implements Function1<SparkSessionExtensions, Unit>
{
    private static final Function1<SparkSession, SparkStrategy> STRATEGY_INJECTOR = NDBStrategy::new;

    public NDBSparkSessionExtension()
    {
    }

    public static String getSessionUser(VastConfig vastConfig)
    {
        SparkSession sparkSession = SparkSession.getActiveSession().get();
        if (!vastConfig.isEndUserImpersonationEnabled()) {
            return null;
        }

        return sparkSession.sqlContext().getConf("spark.sql.session.user",
                SparkContext$.MODULE$.getActive().get().sparkUser());
    }

    public static String getSessionUser(VastConfig vastConfig,
            Map<String, String> sessionConfig)
    {
        if (!vastConfig.isEndUserImpersonationEnabled()) {
            return null;
        }

        return sessionConfig.getOrElse("spark.sql.session.user",
                () -> SparkContext$.MODULE$.getActive().get().sparkUser());
    }

    @Override
    public Unit apply(SparkSessionExtensions sparkSessionExtensions)
    {
        sparkSessionExtensions.injectParser(NDBParser::new);
        sparkSessionExtensions.injectResolutionRule(
                session -> new NDBRowLevelResolutionRule());
        sparkSessionExtensions.injectResolutionRule(
                NDBTablesResolutionRule::new);
        sparkSessionExtensions.injectResolutionRule(
                sparkSession -> new NDBRCLSResolvedRelationAdaptorRule());
        sparkSessionExtensions.injectResolutionRule(
                session -> new SetUserOnSession());
        sparkSessionExtensions.injectResolutionRule(
                session -> new NonAcidResolutionRule());
        sparkSessionExtensions.injectPlannerStrategy(STRATEGY_INJECTOR);
        return null;
    }
}

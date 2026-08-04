/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.VastConfig;
import ndb.view.NDBViewsResolutionRule;
import org.apache.spark.SparkContext$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.execution.SparkStrategy;
import scala.Function1;
import scala.Unit;

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

    @Override
    public Unit apply(SparkSessionExtensions sparkSessionExtensions)
    {
        sparkSessionExtensions.injectPlannerStrategy(STRATEGY_INJECTOR);
        sparkSessionExtensions.injectResolutionRule(
                NDBViewsResolutionRule::new);
        sparkSessionExtensions.injectParser(NDBParser::new);
        sparkSessionExtensions.injectResolutionRule(
                (session) -> new SetUserOnSession());
        return null;
    }
}

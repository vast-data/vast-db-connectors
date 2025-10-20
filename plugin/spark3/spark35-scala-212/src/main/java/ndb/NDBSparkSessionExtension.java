/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import ndb.view.NDBViewsResolutionRule;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.execution.SparkStrategy;
import scala.Function1;
import scala.Unit;

public class NDBSparkSessionExtension
        implements Function1<SparkSessionExtensions, Unit>
{
    private static final Function1<SparkSession, SparkStrategy> STRATEGY_INJECTOR =
            NDBStrategy::new;

    @Override
    public Unit apply(SparkSessionExtensions sparkSessionExtensions)
    {
        sparkSessionExtensions.injectPlannerStrategy(STRATEGY_INJECTOR);
        sparkSessionExtensions.injectResolutionRule(session -> new NDBRowLevelResolutionRule());
        sparkSessionExtensions.injectResolutionRule(NDBViewsResolutionRule::new);
        sparkSessionExtensions.injectParser(NDBParser::new);
        return null;
    }
}

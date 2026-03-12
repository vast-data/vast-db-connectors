/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.ImmutableList;
import com.vastdata.spark.predicate.VastPredicate;
import org.apache.spark.sql.connector.expressions.filter.AlwaysFalse;

import java.util.List;
import java.util.function.Predicate;

public final class AlwaysFalseFilterUtil
{

    private static final Predicate<List<List<VastPredicate>>> IS_ALWAYS_FALSE = predicates -> predicates.stream()
            .anyMatch(plist -> plist.get(0).getPredicate() instanceof AlwaysFalse);
    private static final ImmutableList<List<VastPredicate>> ALWAYS_FALSE_PREDICATES_LIST = ImmutableList.of(ImmutableList.of(new VastPredicate(new AlwaysFalse(), null, null)));

    private AlwaysFalseFilterUtil() {}

    public static List<List<VastPredicate>> getAlwaysFalsePredicates()
    {
        return ALWAYS_FALSE_PREDICATES_LIST;
    }

    public static boolean isAlwaysFalsePredicate(List<List<VastPredicate>> predicates)
    {
        return IS_ALWAYS_FALSE.test(predicates);
    }

}

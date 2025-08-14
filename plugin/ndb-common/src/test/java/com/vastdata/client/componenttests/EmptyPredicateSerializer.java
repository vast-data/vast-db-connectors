/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.componenttests;

import com.vastdata.client.VastExpressionSerializer;

public class EmptyPredicateSerializer
        extends VastExpressionSerializer
{
    @Override
    public int serialize()
    {
        final int columnOffset = buildColumn(0);
        final int validOffset = buildIsValid(columnOffset);

        return buildAnd(buildOr(validOffset));
    }
}

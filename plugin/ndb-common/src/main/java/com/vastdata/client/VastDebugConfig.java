/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

public class VastDebugConfig
{
    public static final VastDebugConfig DEFAULT = new VastDebugConfig(false, false);

    private final boolean isDisableArrowParsing;
    private final boolean disablePageQueueing;

    public VastDebugConfig(boolean isDisableArrowParsing, boolean disablePageQueueing)
    {
        this.isDisableArrowParsing = isDisableArrowParsing;
        this.disablePageQueueing = disablePageQueueing;
    }

    public boolean isDisableArrowParsing()
    {
        return isDisableArrowParsing;
    }

    public boolean isDisablePageQueueing()
    {
        return disablePageQueueing;
    }
}

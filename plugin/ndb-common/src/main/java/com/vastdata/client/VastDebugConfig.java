/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

public class VastDebugConfig
{
    public static final VastDebugConfig DEFAULT = new VastDebugConfig(false,
            false, false);

    private final boolean isDisableArrowParsing;
    private final boolean disablePageQueueing;
    private final boolean enableServerStatsCollection;

    public VastDebugConfig(boolean isDisableArrowParsing,
            boolean disablePageQueueing, boolean enableServerStatsCollection)
    {
        this.isDisableArrowParsing = isDisableArrowParsing;
        this.disablePageQueueing = disablePageQueueing;
        this.enableServerStatsCollection = enableServerStatsCollection;
    }

    public boolean isDisableArrowParsing()
    {
        return isDisableArrowParsing;
    }

    public boolean isDisablePageQueueing()
    {
        return disablePageQueueing;
    }

    public boolean isEnableServerStatsCollection()
    {
        return enableServerStatsCollection;
    }
}

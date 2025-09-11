/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import io.airlift.configuration.Config;
import io.airlift.log.Logger;

import java.io.Serializable;

public class VastTrinoConfig
    implements Serializable
{
    private static final Logger LOG = Logger.get(VastTrinoConfig.class);

    private boolean enableAccessControl;
    private boolean enableRowColumnSecurity;
    private boolean enableEndUserImpersonation;

    public boolean getEnableAccessControl()
    {
        return enableAccessControl;
    }

    @Config("enable_access_control")
    public VastTrinoConfig setEnableAccessControl(final boolean enableAccessControl)
    {
        this.enableAccessControl = enableAccessControl;
        return this;
    }

    public boolean getEnableRowColumnSecurity()
    {
        return enableRowColumnSecurity;
    }

    @Config("enable_row_column_security")
    public VastTrinoConfig setEnableRowColumnSecurity(final boolean enableRowColumnSecurity)
    {
        this.enableRowColumnSecurity = enableRowColumnSecurity;
        return this;
    }

    public boolean getEnableEndUserImpersonation()
    {
        return enableEndUserImpersonation;
    }

    @Config("enable_end_user_impersonation")
    public VastTrinoConfig setEnableEndUserImpersonation(final boolean enableEndUserImpersonation)
    {
        this.enableEndUserImpersonation = enableEndUserImpersonation;
        return this;
    }

    public boolean isRowColumnSecurityEnabled()
    {
        if (enableRowColumnSecurity && !enableAccessControl) {
            LOG.warn("Skipping row-column-security because vast-security-control is disabled");
        }
        return enableAccessControl && enableRowColumnSecurity;
    }

    public boolean isEndUserImpersonationEnabled()
    {
        if (enableEndUserImpersonation && !enableAccessControl) {
            LOG.warn("Skipping end-user-impersonation because vast-security-control is disabled");
        }
        return enableAccessControl && enableEndUserImpersonation;
    }

    @Override
    public String toString() {
        return "VastTrinoConfig{" +
                "enableAccessControl=" + enableAccessControl +
                ", enableRowColumnSecurity=" + enableRowColumnSecurity +
                ", enableEndUserImpersonation=" + enableEndUserImpersonation +
                '}';
    }
}

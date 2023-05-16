/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import io.airlift.log.Logger;

import java.io.IOException;
import java.util.Objects;

public final class VastMockS3ServerStarter
{
    private static final Logger LOG = Logger.get(VastMockS3ServerStarter.class);

    private VastMockS3ServerStarter() {}

    public static void fromEnv()
    {
        // Valid values: null/empty/true/True/false/False. Other values are resolved as false
        if (Boolean.getBoolean("vast.mockServer.enabled")) {
            Integer mockServerPort = Integer.getInteger("vast.mockServer.port");
            //Valid values: int
            try {
                if (Objects.isNull(mockServerPort)) {
                    LOG.error("Failed starting mock server because port configuration is missing");
                }
                else {
                    try {
                        new VastMockS3Server(mockServerPort).start();
                    }
                    catch (IOException e) {
                        LOG.error(e, "Failed starting mock server");
                    }
                }
            }
            catch (NumberFormatException noANumber) {
                LOG.error(noANumber, "Failed starting mock server because of invalid port configuration");
            }
        }
    }
}

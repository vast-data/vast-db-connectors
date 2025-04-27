package com.vastdata.sparkagent;

import java.lang.instrument.Instrumentation;


public class Agent {
    public static void premain(String agentOps, Instrumentation inst) {
        instrument(agentOps, inst);
    }

    public static void agentmain(String agentOps, Instrumentation inst) {
        instrument(agentOps, inst);
    }

    private static void instrument(String agentArgs, Instrumentation inst) {
        inst.addTransformer(new Transformer());
    }
}


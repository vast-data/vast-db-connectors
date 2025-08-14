/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.sparkagent;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.LoaderClassPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

public class Transformer implements ClassFileTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(Transformer.class);
    private static final String PATCH_CLASS_NAME = "org/apache/spark/sql/execution/dynamicpruning/PartitionPruning$";
    private static final String METHOD_NAME = "$anonfun$hasSelectivePredicate$1";
    // javaassist has a java 5- compiler ...
    // about its quirks please see the javassist documentation here: https://www.javassist.org/tutorial/tutorial2.html
    // $1 below is the first parameter of the function, while @r is the type of the return value.
    // This patch is supposed to fix issue ORION-162386
    private static final String METHOD_BODY =
"{\n" +
"    org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(com.vastdata.sparkagent.Transformer.class);\n" +
"    if ($1 instanceof org.apache.spark.sql.catalyst.plans.logical.Filter) {\n" +
"        org.apache.spark.sql.catalyst.plans.logical.Filter var3 = (org.apache.spark.sql.catalyst.plans.logical.Filter)$1;\n" +
"        return ($r) org.apache.spark.sql.execution.dynamicpruning.PartitionPruning$.MODULE$.isLikelySelective(var3.condition());\n" +
"    }\n" +
"    else if ($1 instanceof org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation) {\n" +
"        org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation scanRel = (org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation)$1;\n" +
"        org.apache.spark.sql.connector.read.Scan scan = scanRel.scan();\n" +
"        Class scanClass = scan.getClass();\n" +
"        if (scanClass.getName().equals(\"com.vastdata.spark.VastScan\")) {\n" +
"            java.lang.reflect.Method[] allMethods = scanClass.getMethods();\n" +
"            for (int i = 0; i < allMethods.length; i++) {\n" +
"                java.lang.reflect.Method m = allMethods[i];\n" +
"                String mname = m.getName();\n" +
"                if (mname.equals(\"hasSelectivePredicate\")) {\n" +
"                    return ($r)m.invoke(scan, null);\n" +
"                }\n" +
"            }\n" +
"        }\n" +
"    }\n" +
"    LOG.info($1.getClass().getName());\n" +
"    return ($r)false;\n" +
"}";

    public Transformer() {
    }
    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        if (className == null) {
            return null;
        }
        if (className.equals(PATCH_CLASS_NAME)) {
            ClassPool classPool = ClassPool.getDefault();
            classPool.appendClassPath(new LoaderClassPath(loader));
            classPool.appendSystemPath();
            try {
                CtClass ctClass = classPool.makeClass(new ByteArrayInputStream(classfileBuffer));
                CtMethod declaredMethod = ctClass.getDeclaredMethod(METHOD_NAME);
                if (declaredMethod != null) {
                    declaredMethod.setBody(METHOD_BODY);
                    LOG.info("Patching: " + METHOD_NAME);
                    return ctClass.toBytecode();
                }
            } catch (Exception e) {
                LOG.warn("Failed to instrument " + PATCH_CLASS_NAME, e);
            }
        }

        return classfileBuffer;
    }
}

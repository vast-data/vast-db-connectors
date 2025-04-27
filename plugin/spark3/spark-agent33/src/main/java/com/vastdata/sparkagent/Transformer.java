package com.vastdata.sparkagent;

import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.CtConstructor;
import javassist.LoaderClassPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Transformer implements ClassFileTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(Transformer.class);
    // With the following patch we get optimisation with dynamic pruning
    private final String patchClassName = "org/apache/spark/sql/execution/dynamicpruning/PartitionPruning$";
    private final String methodName = "$anonfun$hasSelectivePredicate$1";
    // javaassist has a java 5- compiler ...
    // about its quirks please see the javassist documentation here: https://www.javassist.org/tutorial/tutorial2.html
    // $1 below is the first parameter of the function, while @r is the type of the return value.
    // This patch is supposed to fix issue ORION-162386
    private final String methodBody =
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

    // Spark 3.3 doesn't use column-level statistics from V2 connectors.  The following patch tries to fix it.
    private final String v2RelationClassName = "org/apache/spark/sql/execution/datasources/v2/DataSourceV2ScanRelation";
    private final String streamingV2RelationClassName = "org/apache/spark/sql/execution/datasources/v2/StreamingDataSourceV2Relation";
    private final String computeStatsMethodName = "computeStats";

    // The computeStats() method of DataSourceV2ScanRelation & StreamingDataSourceV2Relation should be patched to call
    // the above method
    private final String computeStatsMethodBody =
"{\n" +
"    org.apache.spark.sql.connector.read.Scan scan = $0.scan();\n" +
"    if (scan instanceof org.apache.spark.sql.connector.read.SupportsReportStatistics) {\n" +
"        org.apache.spark.sql.connector.read.SupportsReportStatistics supportsReportStatistics =\n" +
"            (org.apache.spark.sql.connector.read.SupportsReportStatistics)scan;\n" +
"        org.apache.spark.sql.connector.read.Statistics statistics1 = supportsReportStatistics.estimateStatistics();\n" +
"        scala.Option numRows =\n" +
"            statistics1.numRows().isPresent() ? new scala.Some(scala.math.BigInt$.MODULE$.long2bigInt(statistics1.numRows().getAsLong())) : (scala.Option)scala.None$.MODULE$;\n" +
"        scala.runtime.ObjectRef colStats = scala.runtime.ObjectRef.create((scala.collection.immutable.Seq)scala.package$.MODULE$.Seq().empty());\n" +
"       java.lang.reflect.Method[] allMethods = statistics1.getClass().getMethods();\n" +
"       java.util.Map vastColumnStat = null;\n" +
"       for (int i = 0; i < allMethods.length; i++) {\n" +
"           java.lang.reflect.Method m = allMethods[i];\n" +
"           String mname = m.getName();\n" +
"           if (mname.equals(\"columnStats\")) {\n" +
"               vastColumnStat = m.invoke(statistics1, null);\n" +
"               break;\n" +
"	   }\n" +
"       }\n" +
"       if (vastColumnStat != null && !vastColumnStat.isEmpty()) {\n" +
"		java.util.Set ks = vastColumnStat.keySet();\n" +
"		java.util.Iterator iter = ks.iterator();\n" +
"		while (iter.hasNext()) {\n" +
"		    org.apache.spark.sql.connector.expressions.NamedReference key = iter.next();\n" +
"		    org.apache.spark.sql.catalyst.plans.logical.ColumnStat colStat = vastColumnStat.get(key);\n" +
"                   scala.collection.immutable.Seq o = $0.output();\n" +
"		    scala.collection.Iterator oiter = o.iterator();\n" +
"		    while (oiter.hasNext()) {\n" +
"			org.apache.spark.sql.catalyst.expressions.AttributeReference attribute = (org.apache.spark.sql.catalyst.expressions.AttributeReference)oiter.next();\n" +
"			if (attribute.name().equals(key.describe())) {\n" +
"			    colStats.elem = (scala.collection.immutable.Seq)((scala.collection.immutable.Seq)colStats.elem).$colon$plus(scala.Predef.ArrowAssoc$.MODULE$.$minus$greater$extension(scala.Predef$.MODULE$.ArrowAssoc(attribute), colStat));\n" +
"			}\n" +
"		    }\n" +
"		}\n" +
"       }\n" +
"        return new org.apache.spark.sql.catalyst.plans.logical.Statistics(\n" +
"            scala.math.BigInt$.MODULE$.long2bigInt(statistics1.sizeInBytes().orElse($0.conf().defaultSizeInBytes())),\n" +
"            (scala.Option)numRows,\n" +
"            org.apache.spark.sql.catalyst.expressions.AttributeMap$.MODULE$.apply((scala.collection.immutable.Seq)colStats.elem),\n" +
"            org.apache.spark.sql.catalyst.plans.logical.Statistics$.MODULE$.apply$default$4());\n" +
"    } else {\n" +
"	    return new org.apache.spark.sql.catalyst.plans.logical.Statistics(scala.math.BigInt$.MODULE$.long2bigInt($0.conf().defaultSizeInBytes()),\n" +
"									      org.apache.spark.sql.catalyst.plans.logical.Statistics$.MODULE$.apply$default$2(),\n" +
"									      org.apache.spark.sql.catalyst.plans.logical.Statistics$.MODULE$.apply$default$3(),\n" +
"									      org.apache.spark.sql.catalyst.plans.logical.Statistics$.MODULE$.apply$default$4());\n" +
"       }\n" +
"}";

    public Transformer() {
    }
    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        if (className == null) {
            return null;
        }
        if (className.equals(this.patchClassName)) {
            ClassPool classPool = ClassPool.getDefault();
            classPool.appendClassPath(new LoaderClassPath(loader));
            classPool.appendSystemPath();
            try {
                CtClass ctClass = classPool.makeClass(new ByteArrayInputStream(classfileBuffer));
                CtMethod declaredMethod = ctClass.getDeclaredMethod(this.methodName);
                if (declaredMethod != null) {
                    declaredMethod.setBody(this.methodBody);
                    LOG.info("Patching: " + this.methodName);
                    return ctClass.toBytecode();
                }
            } catch (Exception e) {
                LOG.warn("Failed to instrument " + className, e);
            }
        }
        else if (className.equals(this.v2RelationClassName) || className.equals(this.streamingV2RelationClassName)) {
            ClassPool classPool = ClassPool.getDefault();
            classPool.appendClassPath(new LoaderClassPath(loader));
            classPool.appendSystemPath();
            try {
                CtClass ctClass = classPool.makeClass(new ByteArrayInputStream(classfileBuffer));
                CtMethod declaredMethod = ctClass.getDeclaredMethod(this.computeStatsMethodName);
                if (declaredMethod != null) {
                    LOG.info("Patching: " + className + "." + this.computeStatsMethodName);
                    declaredMethod.setBody(this.computeStatsMethodBody);
                    return ctClass.toBytecode();
                }
            } catch (Exception e) {
                LOG.warn("Failed to instrument " + className, e);
            }
        }

        return classfileBuffer;
    }
}

/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.google.common.collect.ImmutableList;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import javassist.bytecode.DuplicateMemberException;
import org.apache.spark.util.ChildFirstURLClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;

import static java.lang.String.format;

/*
    Override "stats" method - add a call to OverrideStatsCache().accept in order to prepopulate table-scan objects statistics cache.
    Idea is to use javaassist library to:
    1. create a new method "overrideWrapper"
    2. add it to class declaration
    3. add a call to the new method before "stats" is executed
    4. write new class bytes to the directory in which the spark-ndb jar file is loaded from
    5. add that class to class loader URLs to be loaded from next time the class is required
    * This only works if Spark's class loader is ChildFirstURLClassLoader (which is the case when spark.driver.userClassPathFirst is true)
 */
public final class SparkClassOverrider
{
    private static final Logger LOG = LoggerFactory.getLogger(SparkClassOverrider.class);

    private static final String METHOD_NAME = "stats";
    private static final String NEW_CLASS_DIR = getNDBPath();
    private static String getNDBPath()
    {
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            String replace = NDB.class.getName().replace(".", "/") + ".class";
            URL resource = cl.getResource(replace);
            String resourcePath = resource.getPath();
            LOG.debug("Found NDB class resource: {}:{} = {}, resource path: {}", replace, resource.getClass(), resource, resourcePath);
            int indexOfJar = resourcePath.indexOf("jar"); // in unitests it is not read from jar but from compiled classes in 'target'
            int indexOfSemiCol = resourcePath.indexOf(":");
            int beginIndex = indexOfSemiCol > 0 ? indexOfSemiCol + 1 : 0;
            String filePath = indexOfJar > 0 ? resourcePath.substring(beginIndex, indexOfJar) : resourcePath.substring(beginIndex);
            LOG.debug("Full NDB class file resource path = {}", filePath);
            String dir = filePath.substring(0, filePath.lastIndexOf("/"));
            LOG.info("Found NDB jar path: {}", dir);
            return dir;
        }
        catch (Exception any) {
            LOG.error("Failed getting NDB jar path", any);
            return "."; // TODO - Improve this bad default - will not cause failure, but the overriding mechanism will not work
        }
    }

    private SparkClassOverrider() {}

    public static void overrideV2Relation()
            throws NotFoundException, CannotCompileException
    {
        if (Thread.currentThread().getContextClassLoader() instanceof ChildFirstURLClassLoader) {
            // overriding implementation is required only for unitests. ci/comp tests use precompiled classes in classpath
            // unitests class loader is AppClassLoader and not ChildFirstURLClassLoader
            return;
        }
//         get the subclass
        ClassPool pool = ClassPool.getDefault();
        ImmutableList<CtClass> classes = ImmutableList.of(
                pool.get("org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation"),
                pool.get("org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation")
        );
        CannotCompileException cannotCompileException = null;
        for (CtClass ctClass: classes) {
            try {
                SparkClassOverrider.overrideImpl(ctClass);
            }
            catch (CannotCompileException e) {
                cannotCompileException = e;
            }
        }
        if (cannotCompileException != null) {
            throw cannotCompileException;
        }
    }

    private static void overrideImpl(CtClass cls)
            throws CannotCompileException
    {
        LOG.debug("overrideImpl: {}", cls.getName());
        CtMethod[] allMethods = cls.getMethods();
        CtMethod declaredStatsMethod = null;
        for (CtMethod method : allMethods) {
            if (method.getName().equalsIgnoreCase(METHOD_NAME)) {
                LOG.debug("Found method: {}={} := {}", method, method.getModifiers(), method.getSignature());
                if (method.getModifiers() == 17 || method.getModifiers() == 1) { // public = 0x0001, final = 0x0010
                    declaredStatsMethod = method;
                }
            }
        }
        if (declaredStatsMethod != null) {
            LOG.error("Adding method declaration of method {} to class: {}", declaredStatsMethod, cls.getName());
            String newHelperMethodCodeStr = "public void overrideWrapper() { new com.vastdata.spark.statistics.OverrideStatsCache().accept(this); }";
            cls.defrost(); // needed in case class has been loaded or overwritten to file already - if it is the second case, a DuplicateMemberException will be gracefully caught.
            CtMethod newOverrideWrapperMethod = CtMethod.make(newHelperMethodCodeStr, cls);
            try {
                cls.addMethod(newOverrideWrapperMethod);
            }
            catch (DuplicateMemberException dup) {
                LOG.error("Class {} have already been overriden", cls.getName());
                return;
            }
            CtMethod delegator = CtNewMethod.delegator(declaredStatsMethod, cls); // delegator from parent to child is needed because stats is a method from a parent trait
            delegator.insertBefore("this.overrideWrapper();");
            cls.addMethod(delegator);
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader instanceof ChildFirstURLClassLoader) {
                try {
                    cls.writeFile(NEW_CLASS_DIR);
                    ChildFirstURLClassLoader urlClassLoader = (ChildFirstURLClassLoader) classLoader;
                    LOG.debug("Current ChildFirst classloader URLS: {}", Arrays.toString(urlClassLoader.getURLs()));
                    Class<? extends ChildFirstURLClassLoader> aClass = urlClassLoader.getClass();
                    Method addURL = aClass.getMethod("addURL", URL.class);
                    addURL.setAccessible(true);
                    addURL.invoke(urlClassLoader, new URL(format("file:%s/%s.class", NEW_CLASS_DIR, cls.getName())));
                }
                catch (IOException e) {
                    throw new RuntimeException(format("Failed overriding class %s. IOException on NEW_CLASS_DIR=%s: %s", cls.getSimpleName(), NEW_CLASS_DIR, e.getMessage()), e);
                }
                catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                    throw new RuntimeException(format("Failed overriding class %s. %s exception: %s", cls.getSimpleName(), e.getClass().getSimpleName(), e.getMessage()), e);
                }
                LOG.info("Successfully overridden Class {}, for ChildFirstURLClassLoader: {}", cls.getSimpleName(), Thread.currentThread().getContextClassLoader());
            }
            else {
                Class<?> aClass = cls.toClass();
                LOG.info("Successfully overridden Class {}:{}, for classloader: {}", aClass.hashCode(), aClass, Thread.currentThread().getContextClassLoader());
            }
        }
        else {
            throw new RuntimeException(format("Method name not found: %s", METHOD_NAME));
        }
    }
}

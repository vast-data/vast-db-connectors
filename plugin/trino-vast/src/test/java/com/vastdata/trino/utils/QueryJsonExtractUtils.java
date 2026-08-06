/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import io.airlift.units.Duration;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * A utility class for processing Trino query JSON files.
 * <p>
 * This class can be run as a standalone application to parse JSON files
 * containing query statistics, extract relevant information, and generate a
 * structured representation of the query plan and performance metrics.
 *
 * <h2>Usage</h2>
 * To use this utility, you need to modify the following variables in the
 * {@code main} method:
 * <ul>
 *     <li>The folder path containing the query JSON files can be passed as the first command-line argument.
 *     The utility will recursively scan this directory.</li>
 *     <li>{@code lastJsonInEachFolder}: A boolean flag. If set to {@code true}, only the
 *     lexicographically last JSON file in each directory is processed. If {@code false},
 *     all JSON files are processed.</li>
 * </ul>
 * After configuring the parameters, run the {@code main} method.
 *
 * <h2>Output</h2>
 * For each processed {@code <query_name>.json} file, a corresponding {@code <query_name>_tree.json}
 * file is generated in the same directory. This file contains a simplified, tree-like
 * structure of the query's operators and statistics.
 * <p>
 * The total execution time of all processed queries is printed to the standard output.
 */
public class QueryJsonExtractUtils
{
    private static final Set<String> CONNECTOR_METRICS_KEYS = Stream
            .of("server_ReadColumnRunTimeMicroSec",
                    "server_SelectRowsRunTimeMicroSec")
            .collect(java.util.stream.Collectors.toSet());
    private static final Set<String> OPERATOR_METRICS_KEYS = Stream
            .of("dynamicFilterSplitsProcessed", "inputDataSize")
            .collect(java.util.stream.Collectors.toSet());

    private QueryJsonExtractUtils()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        // TODO: In order to use this util, please update the following args:
        String folderPath = Optional
                .of(args)
                .filter(a -> a.length > 0)
                .map(a -> a[0])
                .orElse("<add your folder path here>");
        boolean lastJsonInEachFolder = false;

        Path rootDir = Path.of(folderPath);
        long executionTimeNanos = 0;
        for (Path path : getDirFiles(rootDir, lastJsonInEachFolder)) {
            try {
                executionTimeNanos += extract(path);
            }
            catch (Exception e) {
                System.err.println(
                        "Failed to process file " + path + ": " + e.getMessage());
            }
        }
        System.out.println("Total execution time = " + Duration
                .succinctNanos(executionTimeNanos)
                .toString(TimeUnit.SECONDS));
    }

    private static List<Path> getDirFiles(Path file,
                                          boolean lastJsonInEachFolder)
            throws RuntimeException
    {
        List<Path> ret = new ArrayList<>();
        List<Path> paths = Arrays
                .stream(file.toFile().listFiles(new JsonFilter()))
                .map(f ->
                {
                    try {
                        return Path.of(
                                new URI("file://" + f.getAbsolutePath()));
                    }
                    catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
        Stream<Path> filesStream = paths
                .stream()
                .filter(p -> p.toFile().isFile());
        if (lastJsonInEachFolder) {
            filesStream
                    .min((p1, p2) -> -1 * p1
                            .toFile()
                            .getAbsolutePath()
                            .compareTo(p2.toFile().getAbsolutePath()))
                    .ifPresent(ret::add);
        }
        else {
            filesStream.forEach(ret::add);
        }
        paths
                .stream()
                .filter(p -> p.toFile().isDirectory())
                .forEach(directory -> ret.addAll(
                        getDirFiles(directory, lastJsonInEachFolder)));
        return ret;
    }

    private static String prepareFileContent(Path path, boolean shouldTrim)
            throws IOException
    {
        try (InputStream fileInputStream = Files.newInputStream(path)) {
            String content = new String(fileInputStream.readAllBytes(),
                    Charset.defaultCharset());
            if (shouldTrim) {
                content = content.replaceAll("\"\"", "\"");
                if (content.startsWith("\"")) {
                    content = content.substring(1, content.length() - 1);
                }
            }
            return content;
        }
    }

    public static long extract(Path p)
            throws IOException, URISyntaxException
    {
        File f = p.toFile();
        System.out.println("starting " + f);
        ObjectReader objectReader = new ObjectMapper().readerFor(TreeMap.class);
        String filename = f
                .getAbsolutePath()
                .substring(0, f.getAbsolutePath().lastIndexOf("."));
        String suffix = f
                .getAbsolutePath()
                .substring(f.getAbsolutePath().lastIndexOf(".") + 1);
        ContainerNode jsonNode = (ContainerNode) objectReader.readTree(
                Reader.of(prepareFileContent(
                        Path.of(new URI("file://" + filename + "." + suffix)),
                        false)));
        ArrayNode operatorsNode;
        ObjectNode queryStats = (ObjectNode) jsonNode.get("queryStats");
        operatorsNode = (ArrayNode) queryStats.get("operatorSummaries");
        Map<String, ObjectNode> s = new TreeMap<>();
        for (JsonNode node : operatorsNode) {
            ObjectNode convertedNode = convertNode((ObjectNode) node);
            if (convertedNode != null) {
                String key = String.format(Locale.US, "%2d_%2d_%2d_%2d",
                        convertedNode.get("1.stageId").intValue(),
                        convertedNode.get("2.pipelineId").intValue(), 0,
                        convertedNode.get("4.operatorId").intValue());
                s.put(key, convertedNode);
            }
        }
        Map<String, Object> tree = new LinkedHashMap<>();
        addStats(tree, queryStats);
        addOperators(tree, s);
        new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .writeValue(Path
                        .of(new URI("file://" + filename + "_tree.json"))
                        .toFile(), tree);
        return getExecutionTimeNanos(queryStats);
    }

    private static long getExecutionTimeNanos(ObjectNode queryStats)
    {
        Duration duration = Duration.valueOf(
                queryStats.get("executionTime").textValue());
        return duration.roundTo(TimeUnit.NANOSECONDS);
    }

    private static void addStats(Map<String, Object> tree,
                                 ObjectNode queryStats)
    {
        List<String> statsToAdd = List.of("executionTime", "planningTime");
        statsToAdd.forEach(stat -> tree.put(stat, queryStats.get(stat)));
    }

    private static void addOperators(Map<String, Object> tree,
                                     Map<String, ObjectNode> s)
    {
        s.forEach((key, value) ->
        {
            List<String> keyTokens = Splitter.on('_').splitToList(key);
            if (keyTokens.size() < 4) {
                throw new RuntimeException("Expected more parts");
            }
            tree.putIfAbsent(getKey(keyTokens.get(0)), new HashMap<>());
            Map<String, Map<String, Map<String, String>>> stageMap = (Map<String, Map<String, Map<String, String>>>) tree.get(
                    getKey(keyTokens.get(0)));
            stageMap.putIfAbsent(getKey(keyTokens.get(1)), new HashMap<>());
            Map<String, Map<String, String>> pipelineMap = stageMap.get(
                    getKey(keyTokens.get(1)));
            pipelineMap.putIfAbsent(getKey(keyTokens.get(2)), new HashMap<>());
            Map<String, String> alternativeMap = pipelineMap.get(
                    getKey(keyTokens.get(2)));
            JsonNode connectorMetrics = value.get("connectorMetrics");
            StringBuilder moreInfo = new StringBuilder();
            if (connectorMetrics != null) {
                for (String metricKey : CONNECTOR_METRICS_KEYS) {
                    JsonNode metricValue = connectorMetrics.get(metricKey);
                    if (metricValue != null) {
                        String connectorMetricValue = metricValue.get(
                                "total") != null ?
                                metricValue.get("total").asText() :
                                metricValue.get("duration").asText();
                        moreInfo
                                .append(", ")
                                .append(metricKey)
                                .append("=")
                                .append(connectorMetricValue);
                    }
                }
            }
            for (String metricKey : OPERATOR_METRICS_KEYS) {
                JsonNode metricValue = value.get(metricKey);
                if (metricValue != null) {
                    moreInfo
                            .append(", ")
                            .append(metricKey)
                            .append("=")
                            .append(metricValue.asText());
                }
            }
            alternativeMap.put(getKey(keyTokens.get(3)), (value
                    .get("5.operatorType")
                    .textValue() + ": op=" + value.get(
                    "outputPositions") + ": wall=" + value.get(
                    "getOutputWall") + ": getOutputCpu=" + value.get(
                    "getOutputCpu") + ": driver=" + value.get(
                    "totalDrivers") + moreInfo));
        });
    }

    private static String getKey(String key)
    {
        return String.format(Locale.US, "%2s", key.trim());
    }

    private static ObjectNode convertNode(ObjectNode node)
    {
        Map<String, JsonNode> nodeValues = new TreeMap<>();
        Iterator<Map.Entry<String, JsonNode>> elements = node.fields();
        while (elements.hasNext()) {
            Map.Entry<String, JsonNode> entry = elements.next();
            String key = switch (entry.getKey()) {
                case "stageId" -> "1.stageId";
                case "pipelineId" -> "2.pipelineId";
                case "alternativeId" -> "3.alternativeId";
                case "operatorId" -> "4.operatorId";
                case "operatorType" -> "5.operatorType";
                default -> entry.getKey();
            };
            if (key.equals("metrics") || key.equals("info")) {
                continue;
            }
            JsonNode value = entry.getValue();
            if (key.contains("operatorType") && value
                    .asText()
                    .contains("Exchange")) {
                return null;
            }
            if (value instanceof ObjectNode objectNode) {
                value = convertNode(objectNode);
            }
            nodeValues.put(key, value);
        }
        ObjectNode ret = new ObjectNode(JsonNodeFactory.instance);
        nodeValues.forEach(ret::replace);
        return ret;
    }

    private static class JsonFilter
            implements FilenameFilter
    {
        @Override
        public boolean accept(File dir, String name)
        {
            try {
                return ((name.endsWith(".txt") || name.endsWith(
                        ".json")) && !name.endsWith(
                        "_tree.json") && !name.endsWith(
                        "_orig.json") && !name.endsWith("_sort.json")) || Path
                        .of(new URI(
                                "file://" + dir.getAbsolutePath() + "/" + name))
                        .toFile()
                        .isDirectory();
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

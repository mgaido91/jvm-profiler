package it.agile.profiling.reporters;

import com.uber.profiling.ArgumentUtils;
import com.uber.profiling.Reporter;
import com.uber.profiling.util.AgentLogger;
import com.uber.profiling.util.JsonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class HdfsReporter implements Reporter {
    final static String ARG_OUTPUT_DIR = "outputDir";
    final static long FLUSH_SIZE = 30;
    private static final AgentLogger logger = AgentLogger.getLogger(HdfsReporter.class.getName());
    private ConcurrentHashMap<String, FSDataOutputStream> fileWriters = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LongAdder> metricsCounters = new ConcurrentHashMap<>();

    private String baseOutputDirectory;
    private Configuration conf = new Configuration();
    private FileSystem dfs;
    private String containerId = System.getenv("CONTAINER_ID");
    private boolean closed = false;

    private synchronized FileSystem getFileSystem() {
        if (dfs == null) {
            try {
                dfs = FileSystem.get(conf);
            } catch (IOException e) {
                logger.warn("Failed getting FileSystem", e);
            }
        }
        return dfs;
    }


    @Override
    public void updateArguments(Map<String, List<String>> parsedArgs) {
        String outDir = ArgumentUtils.getArgumentSingleValue(parsedArgs, ARG_OUTPUT_DIR);
        if (ArgumentUtils.needToUpdateArg(outDir)) {
            baseOutputDirectory = outDir;
            logger.info("Got argument value for \"" + ARG_OUTPUT_DIR + "\": " +
                    baseOutputDirectory);
            Path hdfsPath = new Path(baseOutputDirectory);
            FileSystem fs = getFileSystem();
            for (int i = 0; i < 3; ++i) {
                try {
                    if (!fs.exists(hdfsPath)) {
                        fs.mkdirs(hdfsPath);
                    }
                } catch (IOException e) {
                    logger.warn("Exception creating directory " + i + " times.", e);
                }
            }
        } else {
            throw new RuntimeException("No value specified for \"" + ARG_OUTPUT_DIR + "\".");
        }
    }

    @Override
    public void report(String profilerName, Map<String, Object> metrics) {
        if (closed) {
            logger.warn("Report already closed, do not report metrics");
            return;
        }
        FSDataOutputStream writer = ensureFile(profilerName);
        try {
            writer.writeBytes(JsonUtils.serialize(metrics));
            writer.writeBytes(System.lineSeparator());
            metricsCounters.computeIfAbsent(profilerName, k -> new LongAdder()).increment();
            LongAdder profilerCounter = metricsCounters.get(profilerName);
            // Flushing every about FLUSH_SIZE records. For concurrency reasons we may do so less
            // often than expected but it is not an issue.
            if (profilerCounter.sum() > FLUSH_SIZE) {
                profilerCounter.reset();
                writer.flush();
            }
        } catch (IOException e) {
            logger.warn("Error while writing metrics to HDFS.", e);
        }
    }

    @Override
    public void close() {
        closed = true;

        List<FSDataOutputStream> copy = new ArrayList<>(fileWriters.values());
        for (FSDataOutputStream writer : copy) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                logger.warn("Error closing the file writer.", e);
            }
        }
    }

    private FSDataOutputStream ensureFile(String profilerName) {
        String fileName = baseOutputDirectory + "/" + profilerName + "_" + containerId + ".json";
        return fileWriters.computeIfAbsent(fileName, this::createFileWriter);
    }

    private FSDataOutputStream createFileWriter(String fileName) {
        Path path = new Path(fileName);
        try {
            return getFileSystem().create(path, false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create file writer: " + path, e);
        }
    }
}

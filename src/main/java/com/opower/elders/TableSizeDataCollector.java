package com.opower.elders;

import akka.actor.ActorSystem;
import akka.util.Duration;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;
import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.apache.commons.lang3.StringUtils.trim;

/**
 * Command-line tool that polls hdfs for hbase table physical sizes on disk.
 * This is meant to be consumed by the d3 animation render tool.
 *
 * @author alexandre.normand
 */
public final class TableSizeDataCollector {
    private static final Log LOG = LogFactory.getLog(TableSizeDataCollector.class);

    @Parameter(names = { "-interval"}, description = "Poll interval in seconds (default: 30)")
    Integer interval = 30;

    @Parameter(names = "-output", description = "Output file", required = true, converter = FileConverter.class)
    File outputFile;

    @Parameter(names = "-hbaseRoot", description = "The hdfs path for the hbase root (default: /hbase)")
    String hbaseRoot = "/hbase";

    @Parameter(names = "-tableNames", description = "Table names to report data for (space delimited)", variableArity = true,
        required = true)
    List<String> tableNames = new ArrayList<String>();

    public static void main(String []argv) throws Exception {
        TableSizeDataCollector tableSizeDataCollector = new TableSizeDataCollector();
        JCommander jCommander = new JCommander(tableSizeDataCollector);
        try {
            jCommander.parse(argv);
        }
        catch (ParameterException e) {
            jCommander.usage();
            System.exit(1);
        }

        TableSizeDataPoller tableSizeDataPoller = new TableSizeDataPoller(tableSizeDataCollector.hbaseRoot,
            tableSizeDataCollector.interval, tableSizeDataCollector.tableNames, tableSizeDataCollector.outputFile);
        tableSizeDataPoller.run();
    }

    /**
     * This polls hdfs looking for the size of hbase tables. It does so by running hadoop fs -du -s
     * on each directory that physically holds the hbase table. It writes the data out to the
     * {@link com.opower.elders.TableSizeDataCollector#outputFile} on exit. It is therefore important
     * that the process is stopped gracefully: either with ctrl-c if running in foreground
     * or kill -2 <pid> if running in background.
     *
     * @author alexandre.normand
     */
    public static class TableSizeDataPoller implements Runnable {

        public static final ActorSystem ACTOR_SYSTEM = ActorSystem.create();
        public static final String SIZE = "size";
        private String hbaseRoot;
        private int pollInterval;
        private List<String> tablesToPoll;
        private ObjectMapper objectMapper = new ObjectMapper();
        private final ObjectWriter writer = this.objectMapper.writerWithDefaultPrettyPrinter();
        private final List<NamedData> data = newArrayList();
        private final Map<String, NamedData> lookupTable = newHashMap();

        public TableSizeDataPoller(String hbaseRoot, int pollInterval, List<String> tablesToPoll, final File outputFile) {
            this.hbaseRoot = hbaseRoot;
            this.pollInterval = pollInterval;
            this.tablesToPoll = tablesToPoll;
            initalizeNamedData();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        FileWriter fileWriter = new FileWriter(outputFile);
                        objectMapper.writeValue(fileWriter, data);

                    }
                    catch (IOException e) {
                        Throwables.propagate(e);
                    }
                }
            });
        }

        private void initalizeNamedData() {
            for (String table : this.tablesToPoll) {
                NamedData namedData = initializeMetrics(table);
                this.data.add(namedData);
            }
        }

        private NamedData initializeMetrics(String name) {
            Map<String, List<Number[]>> metrics = newHashMap();
            ArrayList<Number[]> size = newArrayList();
            metrics.put(SIZE, size);

            NamedData namedData = new NamedData(name, metrics);
            lookupTable.put(name, namedData);
            return namedData;
        }

        public void run() {
            LOG.debug("Reading all tables...");
            Map<String, Long> tables = getTableSizes();
            try {
                Map<Long, Map<String, Long>> pollResult = newHashMap();
                long currentTime = new Date().getTime();
                pollResult.put(currentTime, tables);
                String content = writer.writeValueAsString(pollResult);
                System.out.println(content);

                // append data
                appendData(currentTime, tables);
            }
            catch (IOException e) {
                LOG.error("Failed to serialize output", e);
            }
            LOG.debug("Refreshed size of all tables.");
            LOG.debug(format("Scheduling next refresh in %d seconds...", this.pollInterval));

            ACTOR_SYSTEM.scheduler().scheduleOnce(
                    Duration.create(this.pollInterval, TimeUnit.SECONDS),
                    this);
        }

        private Map<String, Long> getTableSizes() {
            Map<String, Long> tableSizes = newHashMap();
            for (String table : this.tablesToPoll) {
                try {
                    String path = format("%s/%s", this.hbaseRoot, table);
                    ProcessBuilder processBuilder = new ProcessBuilder("hadoop", "fs", "-du", "-s", path);
                    Process process = processBuilder.start();
                    process.waitFor();

                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    StringBuilder builder = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        builder.append(line);
                        builder.append(System.getProperty("line.separator"));
                    }
                    String result = builder.toString();
                    long size = Long.parseLong(trim(substringBefore(result, "/")));
                    tableSizes.put(table, size);
                }
                catch (Exception e) {
                    LOG.error(format("Error getting table size for %s", table), e);
                    tableSizes.put(table, -1L);
                }
            }
            return tableSizes;
        }

        private void appendData(Long timestamp, Map<String, Long> tables) {
            for (String job : this.tablesToPoll) {
                Long tableSize = tables.get(job);
                NamedData tableData = lookupTable.get(job);
                appendMetricData(timestamp, tableSize, tableData.getData());
            }
        }

        private void appendMetricData(Long timestamp, Long size, Map<String, List<Number[]>> jobData) {
            Long tableSize = 0L;
            if (size != null) {
                tableSize = size;
            }

            addTimestampedValue(jobData.get(SIZE), timestamp, tableSize);
        }

        private void addTimestampedValue(List<Number[]> metricValues, Long timestamp, Number value) {
            Number []values = new Number[2];
            values[0] = timestamp;
            values[1] = zeroIfNull(value);
            metricValues.add(values);
        }

        public static Number zeroIfNull(Number value) {
            return value == null ? 0 : value;
        }
    }

    /**
     * Named object to match expected json model
     * @author alexandre.normand
     */
    public static class NamedData {
        private String name;
        Map<String, List<Number[]>> data;

        public NamedData(String name, Map<String, List<Number[]>> data) {
            this.name = name;
            this.data = data;
        }

        public String getName() {
            return name;
        }

        Map<String, List<Number[]>> getData() {
            return data;
        }

        @JsonAnyGetter
        public Map<String, List<Number[]>> any() {
            return data;
        }

        @JsonAnySetter
        public void set(String name, List<Number[]> value) {
            this.data.put(name, value);
        }
    }
}

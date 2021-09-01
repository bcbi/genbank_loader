/*
 * Copyright 2015 The University of Vermont and State
 * Agricultural College.  All rights reserved.
 *
 * Written by Matthew B. Storer <matthewbstorer@gmail.com>
 *
 * This file is part of CCTS Common.
 *
 * CCTS Common is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CCTS Common is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with CCTS Common.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.uvm.ccts.genbank.db.loader;

import edu.uvm.ccts.common.db.parser.AbstractFileParser;
import edu.uvm.ccts.common.db.DataSource;
import edu.uvm.ccts.common.model.FileMetadata;
import edu.uvm.ccts.common.util.DBUtil;
import edu.uvm.ccts.common.util.FileUtil;
import edu.uvm.ccts.common.util.TimeUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by mstorer on 5/16/14.
 */
public abstract class AbstractLoader {
    private static final Log log = LogFactory.getLog(AbstractLoader.class);

    private static final int MB = (int) Math.pow(2, 20);

    protected abstract String getName();
    protected abstract int getRequiredMemPerThreadMB();
    protected abstract AbstractFileParser buildParser(int threadId, String tempDir) throws IOException;
    protected abstract Map<String, String> getTableFileMap();
    protected abstract Callable<Object> buildWorkerThreadLogic(final int threadId, final String tempDir);

    private final List<FileMetadata> queue = new ArrayList<FileMetadata>();
    private int currentQueueIndex = 0;


    /**
     * Main process for bulk-loading data from generated files into the database
     * @throws java.sql.SQLException
     */
    public void populateDatabase(DataSource dataSource, String dir) throws SQLException, IOException {
        log.info("populating database from '" + dir + "' -");
        long start = System.currentTimeMillis();

        File f = new File(dir);
        if ( ! f.isDirectory() ) {
            throw new FileNotFoundException("directory '" + f.getCanonicalPath() + "' does not exist");
        }

        Map<String, String> tableFileMap = getTableFileMap();
        for (Map.Entry<String, String> entry : tableFileMap.entrySet()) {
            dbLoad(dataSource, entry.getKey(), dir + "/" + entry.getValue());
        }

        log.info("finished populating database.  took " + TimeUtil.formatMsToHMS(System.currentTimeMillis() - start));
    }

    /**
     * Bulk-loads data from an individual file into an individual database table
     * @param table the name of the table to populate
     * @param filename the name of the file from which data will be loaded into the specified table
     * @throws SQLException
     */
    private void dbLoad(DataSource dataSource, String table, String filename) throws SQLException {
        log.info(" loading '" + filename + "' into table '" + table + "'");
        //DBUtil.executeUpdate("alter table " + table + " disable keys", dataSource);
        DBUtil.executeUpdate("set session sql_log_bin = OFF", dataSource);
        DBUtil.executeUpdate("delete from " + table, dataSource);
        DBUtil.executeUpdate("load data local infile '" + filename + "' into table " + table, dataSource);
        DBUtil.executeUpdate("set session sql_log_bin = ON", dataSource);
        log.info(" building indexes for table '" + table + "'");
        //DBUtil.executeUpdate("alter table " + table + " enable keys", dataSource);
    }

    protected void addQueueItem(FileMetadata fileMetadata) {
        queue.add(fileMetadata);
    }

    protected int getQueueSize() {
        return queue.size();
    }

    /**
     * Acquires the next item in the download queue, if one exists.  When an item is acquired from the download queue,
     * it is removed from the queue.  Therefore, each call to this function will decrement the size of the download
     * queue by 1 until the queue is empty.
     * @return an instance of {@link QueueItem} if the queue contained at least one item, or {@code null} if the queue
     * was empty.
     */
    protected QueueItem popNextQueueItem() {
        synchronized(queue) {
            if (queue.size() > 0 && currentQueueIndex < queue.size()) {
                FileMetadata item = queue.get(currentQueueIndex ++);
                return new QueueItem(item, currentQueueIndex);

            } else {
                return null;
            }
        }
    }

    /**
     * Handles invocation, error handling, and shutdown of worker threads
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    protected void invoke() throws InterruptedException {
        if (getAllocatedMemMB() < getRequiredMemPerThreadMB()) {
            double mult = 2.5;
            int suggestedAlloc = (int) (getRequiredMemPerThreadMB() * mult);

            log.warn("*** INSUFFICIENT JAVA HEAP MEMORY ALLOCATED! ***");
            log.warn("This process requires at least " + getRequiredMemPerThreadMB() + " megabytes of memory per thread, but only " + getAllocatedMemMB() + " megabytes has been allocated to the JVM.");
            log.warn("As a result, this process may experience severely degraded performance, and may fail with an OutOfMemoryError unless");
            log.warn("corrected.  To allocate more memory to the Java heap, use the '-Xmx' option.  It is suggested to allocate at least " + mult);
            log.warn("times the required memory per thread to the JVM, but more is better and will translate to improved performance,");
            log.warn("especially on systems with many CPU cores.  To allocate the minimum suggested memory to this process, use '-Xmx" + suggestedAlloc + "m'.");
        }

        List<Callable<Object>> threads = new ArrayList<Callable<Object>>();

        int threadCount = getThreadCount();
        for (int i = 1; i <= threadCount; i ++) {
            threads.add(new WorkerThread(i));
        }

        if (threads.isEmpty()) return;

        long start = System.currentTimeMillis();

        ExecutorService svc = Executors.newFixedThreadPool(threads.size());

        try {
            List<Future<Object>> futures = svc.invokeAll(threads);

            for (int i = 0; i < futures.size(); i ++) {
                Future future = futures.get(i);
                try {
                    future.get();

                } catch (Exception e) {
                    log.info("thread[" + (i + 1) + "] : caught " + e.getClass().getName() + " - " + e.getMessage(), e);
                }
            }

        } finally {
            svc.shutdownNow();
        }

        log.info("processing " + queue.size() + " files across " + threads.size() + " threads took " +
                TimeUtil.formatMsToHMS(System.currentTimeMillis() - start));
    }

    /**
     * Updates the current local metadata file of successfully-processed remote files
     * @param fileMetadata The file metadata to append to the local metadata file
     * @throws IOException
     */
    protected void writeMetadata(String file, FileMetadata fileMetadata) throws IOException {
        FileUtil.write(file, fileMetadata.serialize() + "\n", true);
    }

    private int getThreadCount() {
        int coreCount = Runtime.getRuntime().availableProcessors();

        if (coreCount <= 2) return 1;           // if system is single or dual-core, regardless of any memory
                                                // constraints, we want only one thread.

        int avail = getAllocatedMemMB();
        int reqPerThread = getRequiredMemPerThreadMB();

        int threads = (int) ((double) avail / reqPerThread);        // # threads w/ sufficient mem that can
                                                                    // be created in avail mem space

        threads = Math.max(1, Math.min(threads, coreCount - 2));    // keep at least 2 cores free for system and
                                                                    // other user processes, but require that at
                                                                    // least one thread be created

        return Math.min(threads, getQueueSize());                   // don't spin up threads if there aren't queue
                                                                    // items to use them

    }

    private int getAllocatedMemMB() {
        long allocated = Runtime.getRuntime().maxMemory();
        return (int) ((double) allocated / MB);
    }

    /**
     * Represents the abstraction of metadata - simple wrapper for loading, accessing, and writing
     */
    protected static final class Metadata {
        private Map<String, FileMetadata> map;

        public Metadata(String filename) throws IOException {
            map = new HashMap<String, FileMetadata>();
            for (FileMetadata meta : loadMetadata(filename)) {
                map.put(meta.getFilename(), meta);
            }
        }

        public FileMetadata get(String filename) {
            return map.get(filename);
        }

        public FileMetadata get(FileMetadata fileMetadata) {
            return map.get(fileMetadata.getFilename());
        }

        private List<FileMetadata> loadMetadata(String filename) throws IOException {
            List<FileMetadata> list = new ArrayList<FileMetadata>();

            if (FileUtil.exists(filename)) {
                for (String line : FileUtil.readLines(filename)) {
                    FileMetadata metadata = FileMetadata.deserialize(line);
                    list.add(metadata);
                }
            }

            return list;
        }
    }


    /**
     * A helper class that simply wraps the most recently popped {@link edu.uvm.ccts.common.model.FileMetadata} object
     * from the download queue, along with its index in the download queue for reporting purposes.
     */
    protected static final class QueueItem {
        private FileMetadata item;
        private int number;

        public QueueItem(FileMetadata item, int number) {
            this.item = item;
            this.number = number;
        }

        public FileMetadata getItem() {
            return item;
        }

        public int getNumber() {
            return number;
        }
    }

    private class WorkerThread implements Callable<Object> {
        private int threadId;

        private WorkerThread(int threadId) {
            this.threadId = threadId;
        }

        @Override
        public Object call() throws Exception {
            Path tempPath = Files.createTempDirectory(getName());

            try {
                Callable<Object> logic = buildWorkerThreadLogic(threadId, tempPath.toString());
                logic.call();

            } catch (Error e) {
                log.error("[" + threadId + "]  encountered " + e.getClass().getName() + " - " + e.getMessage(), e);
                throw e;

            } catch (Exception e) {
                log.error("[" + threadId + "]  encountered " + e.getClass().getName() + " - " + e.getMessage(), e);
                throw e;

            } finally {
                try { tempPath.toFile().delete(); } catch (Exception e) {}
            }

            return null;
        }
    }
}

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
import edu.uvm.ccts.common.util.FileUtil;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Created by mstorer on 5/16/14.
 */
public abstract class AbstractFileLoader extends AbstractLoader {
    private static final Log log = LogFactory.getLog(AbstractFileLoader.class);

    private String outputDir;
    private String localMetadataFile;

    protected abstract String getFilenameFilter();

    protected AbstractFileLoader(String outputDir) {
        this.outputDir = outputDir;
        localMetadataFile = outputDir + "/.metadata";
    }

    public void prepare(String path) throws InterruptedException, IOException {
        populateFileQueue(path);
        invoke();
    }

    public void populateDatabase(DataSource dataSource) throws IOException, SQLException {
        super.populateDatabase(dataSource, outputDir);
    }


//////////////////////////////////////////////////////////////////////////////////////////
// private methods
//

    private void populateFileQueue(String path) throws IOException {
        Metadata metadata = new Metadata(localMetadataFile);

        File f = new File(path);
        if (f.isDirectory()) {
            FileFilter filter = new WildcardFileFilter(getFilenameFilter());
            for (File current : FileUtil.listFiles(path, filter)) {
                processFileToQueue(metadata, current);
            }

        } else if (f.isFile()) {
            processFileToQueue(metadata, f);
        }
    }

    private void processFileToQueue(Metadata metadata, File f) throws IOException {
        FileMetadata current = new FileMetadata(f);
        FileMetadata existing = metadata.get(current);
        if (existing != null && existing.equals(current)) {
            log.info("skipping file " + current.getFilename() + " - no changes detected");
            writeMetadata(localMetadataFile, current);

        } else {
            addQueueItem(current);
        }
    }

    @Override
    protected Callable<Object> buildWorkerThreadLogic(final int threadId, final String tempDir) {
        return new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                AbstractFileParser parser = buildParser(threadId, tempDir);

                QueueItem item;
                while ((item = popNextQueueItem()) != null) {
                    FileMetadata meta = item.getItem();
                    int i = item.getNumber();

                    try {
                        String filename = FileUtil.getFilenamePart(meta.getFilename());

                        int pct = (int) (((float) i / getQueueSize()) * 100);
                        String pctCompletedStr = i + "/" + getQueueSize() + ", " + pct + "%";

                        log.info("[" + threadId + "]  (" + pctCompletedStr + ")  processing '" + filename + "'");

                        parser.parse(meta.getFilename());

                        writeMetadata(localMetadataFile, meta);

                    } catch (Exception e) {
                        log.error("[" + threadId + "]  caught " + e.getClass().getName() + " processing " +
                                meta.getFilename() + " - " + e.getMessage(), e);
                    }
                }

                log.info("[" + threadId + "]  done.");

                return null;
            }
        };
    }
}

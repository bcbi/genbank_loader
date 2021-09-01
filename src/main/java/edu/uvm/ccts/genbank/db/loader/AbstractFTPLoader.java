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
import edu.uvm.ccts.common.ftp.FTPClient;
import edu.uvm.ccts.common.model.FileMetadata;
import edu.uvm.ccts.common.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by mstorer on 5/19/14.
 */
public abstract class AbstractFTPLoader extends AbstractLoader {
    private static final Log log = LogFactory.getLog(AbstractFTPLoader.class);

    private String outputDir;
    private String localMetadataFile;

    protected AbstractFTPLoader(String outputDir) {
        this.outputDir = outputDir;
        localMetadataFile = outputDir + "/.metadata";
    }

    protected abstract String getFTPHost();
    protected abstract String getFTPUser();
    protected abstract String getFTPPass();
    protected abstract List<String> getFilenameFilters();


    /**
     * This function implements the second level of execution.  A list of files to download is generated,
     * those files are processed, and the database is subsequently populated.
     * @throws Exception
     */
    public void prepare() throws Exception {
        FTPClient ftp = null;

        try {
            ftp = new FTPClient(getFTPHost(), getFTPUser(), getFTPPass());
            ftp.connect();

            populateDownloadQueue(ftp);

        } finally {
            try { if (ftp != null) ftp.disconnect(); } catch (Exception e) {}
        }

        invoke();
    }

    public void populateDatabase(DataSource dataSource) throws IOException, SQLException {
        super.populateDatabase(dataSource, outputDir);
    }


//////////////////////////////////////////////////////////////////////////////////////////
// private methods
//

    /**
     * Identifies which files need to be downloaded.  This involves going out to the NIH FTP
     * server, retrieving files by filename-mask match, and comparing their metadata to locally-
     * stored copies.  Files are flagged for download if their local metadata is missing or
     * different.
     * @param ftp An {@link FTPClient} instance
     * @throws IOException
     */
    private void populateDownloadQueue(FTPClient ftp) throws IOException {
        Metadata metadata = new Metadata(localMetadataFile);

        for (String filenameFilter : getFilenameFilters()) {
            for (FileMetadata remote : ftp.listFilesWithMetadata(filenameFilter)) {
                FileMetadata local = metadata.get(remote);
                if (local != null && local.equals(remote)) {
                    log.info("skipping file " + remote.getFilename() + " - no changes detected");
                    writeMetadata(localMetadataFile, remote);

                } else {
                    addQueueItem(remote);
//                    break;                  // todo : remove after testing
                }
            }
//            break;                          // todo : remove after testing
        }
    }

    @Override
    protected Callable<Object> buildWorkerThreadLogic(final int threadId, final String tempDir) {
        return new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                FTPClient ftp = null;

                try {
                    log.info("[" + threadId + "]  start");

                    ftp = new FTPClient(getFTPHost(), getFTPUser(), getFTPPass());
                    ftp.connect();

                    AbstractFileParser parser = buildParser(threadId, tempDir);

                    QueueItem item;
                    while ((item = popNextQueueItem()) != null) {
                        FileMetadata remote = item.getItem();
                        int i = item.getNumber();

                        String tmpFilename = null;

                        try {
                            int pct = (int) (((float) i / getQueueSize()) * 100);
                            String pctCompletedStr = i + "/" + getQueueSize() + ", " + pct + "%";

                            String filename = FileUtil.getFilenamePart(remote.getFilename());
                            tmpFilename = tempDir + "/" + filename;

                            log.info("[" + threadId + "]  (" + pctCompletedStr + ")  processing '" + filename + "'");
                            ftp.download(remote.getFilename(), tmpFilename);

                            parser.parse(tmpFilename);

                            writeMetadata(localMetadataFile, remote);

                        } catch (Exception e) {
                            log.error("[" + threadId + "]  caught " + e.getClass().getName() + " processing " +
                                    remote.getFilename() + " - " + e.getMessage(), e);

                        } finally {
                            if (tmpFilename != null) FileUtil.delete(tmpFilename);
                        }
                    }

                    log.info("[" + threadId + "]  done.");

                } finally {
                    try { if (ftp != null) ftp.disconnect(); } catch (Exception e) {}
                }

                return null;
            }
        };
    }
}

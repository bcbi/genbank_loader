/*
 * Copyright 2015 The University of Vermont and State Agricultural
 * College, Vermont Oxford Network.  All rights reserved.
 *
 * Written by Matthew B. Storer <matthewbstorer@gmail.com>
 *
 * This file is part of GenBank Loader.
 *
 * GenBank Loader is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GenBank Loader is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with GenBank Loader.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.uvm.ccts.genbank;

import edu.uvm.ccts.genbank.db.loader.AbstractFTPLoader;
import edu.uvm.ccts.common.db.parser.AbstractFileParser;
import edu.uvm.ccts.common.ftp.FTPClient;
import edu.uvm.ccts.common.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

/**
 * Handles main-process execution of determining what to download, where to put generated data,
 * and loading generated data into the target data source
 */
public class MetaGenbankLoader extends AbstractFTPLoader {
    private static final Log log = LogFactory.getLog(MetaGenbankLoader.class);

    private static final String outputDir = "./out";

    private Properties properties;


    private static final Map<String, String> tableFileMap = new LinkedHashMap<String, String>();
    static {
        tableFileMap.put("basic", FeatureTableParser.TABLE_FILE_BASIC);
        tableFileMap.put("keywords", FeatureTableParser.TABLE_FILE_KEYWORDS);
        tableFileMap.put("dbxrefs", FeatureTableParser.TABLE_FILE_DBXREFS);
        tableFileMap.put("journals", FeatureTableParser.TABLE_FILE_JOURNALS);
        tableFileMap.put("authors", FeatureTableParser.TABLE_FILE_AUTHORS);
        tableFileMap.put("annotations", FeatureTableParser.TABLE_FILE_ANNOTATIONS);
    }


    public MetaGenbankLoader() throws IOException {
        super(outputDir);

        properties = new Properties();
        properties.load(this.getClass().getResourceAsStream("/system.properties"));
    }

    @Override
    protected String getName() {
        return "genbank";
    }

    @Override
    protected int getRequiredMemPerThreadMB() {
        return 400;
    }

    @Override
    protected AbstractFileParser buildParser(int threadId, String tempDir) throws IOException {
        return new FeatureTableParser(threadId, tempDir, outputDir);
    }

    @Override
    protected Map<String, String> getTableFileMap() {
        return tableFileMap;
    }

    @Override
    protected String getFTPHost() {
        return properties.getProperty("nih.ftp.host");
    }

    @Override
    protected String getFTPUser() {
        return properties.getProperty("nih.ftp.user");
    }

    @Override
    protected String getFTPPass() {
        return properties.getProperty("nih.ftp.pass");
    }

    @Override
    protected List<String> getFilenameFilters() {
        return Arrays.asList(
                "/genbank/gb*.seq.gz",
                "/refseq/release/complete/complete*.*.gbff.gz");
    }

    @Override
    public void prepare() throws Exception {
        FTPClient ftp = null;

        try {
            String relNoFile = outputDir + "/.current-release";

            String currentRelNo = null;
            if (FileUtil.exists(relNoFile)) {
                currentRelNo = FileUtil.read(relNoFile).trim();
            }

            ftp = new FTPClient(getFTPHost(), getFTPUser(), getFTPPass());
            ftp.connect();

            String tmpFilename = Files.createTempFile(null, null).toString();

            ftp.download("/genbank/GB_Release_Number", tmpFilename);
            
            String relNo = FileUtil.read(tmpFilename).trim();

            if (currentRelNo == null) {
                FileUtil.createDirectory(outputDir);
                FileUtil.moveFile(tmpFilename, relNoFile);

            } else if ( ! currentRelNo.equals(relNo) ) {
                log.info("remote release has been updated from " + currentRelNo + " to " + relNo +
                        " - clearing output directory " + outputDir);

                FileUtil.removeDirectory(outputDir);
                FileUtil.createDirectory(outputDir);
                FileUtil.moveFile(tmpFilename, relNoFile);

            } else {
                FileUtil.delete(tmpFilename);
            }

        } finally {
            try { if (ftp != null) ftp.disconnect(); } catch (Exception e) {}
        }

        super.prepare();
    }
}

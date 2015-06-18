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

import edu.uvm.ccts.common.db.parser.AbstractCustomFileParser;
import edu.uvm.ccts.common.db.parser.TableData;
import edu.uvm.ccts.common.util.FileUtil;
import edu.uvm.ccts.genbank.model.DbXRef;
import edu.uvm.ccts.genbank.model.Journal;
import edu.uvm.ccts.genbank.model.Record;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Parses a feature table file, during which process records are extracted and written to file
 * in a format appropriate for bulk-loading into database tables.
 */
public class FeatureTableParser extends AbstractCustomFileParser {
    public static final String TABLE_FILE_BASIC = "basic.txt";
    public static final String TABLE_FILE_KEYWORDS = "keywords.txt";
    public static final String TABLE_FILE_DBXREFS = "dbxrefs.txt";
    public static final String TABLE_FILE_JOURNALS = "journals.txt";
    public static final String TABLE_FILE_AUTHORS = "authors.txt";
    public static final String TABLE_FILE_ANNOTATIONS = "annotations.txt";

    private static final int ANNOTATION_VALUE_INDEX_LEN = 100;

    private TableData tBasic;
    private TableData tKeywords;
    private TableData tDbXRef;
    private TableData tJournals;
    private TableData tAuthors;
    private TableData tAnnotations;
    private List<TableData> tableDataList;


    public FeatureTableParser(int threadId, String tempDir, String outputDir) throws IOException {
        super(threadId);

        tBasic =        new TableData(tempDir, outputDir, TABLE_FILE_BASIC);
        tKeywords =     new TableData(tempDir, outputDir, TABLE_FILE_KEYWORDS);
        tDbXRef =       new TableData(tempDir, outputDir, TABLE_FILE_DBXREFS);
        tJournals =     new TableData(tempDir, outputDir, TABLE_FILE_JOURNALS);
        tAuthors =      new TableData(tempDir, outputDir, TABLE_FILE_AUTHORS);
        tAnnotations =  new TableData(tempDir, outputDir, TABLE_FILE_ANNOTATIONS);

        tableDataList = Arrays.asList(tBasic, tKeywords, tDbXRef, tJournals, tAuthors, tAnnotations);
    }

    @Override
    public void parse(String filename) throws Exception {
        extractFeatureTableData(filename);
        super.parse(filename);
    }

    @Override
    protected List<TableData> getTableDataList() {
        return tableDataList;
    }

    @Override
    protected InputStream getInputStream(String filename) throws IOException {
        return new GZIPInputStream(new FileInputStream(filename));
    }

    @Override
    protected String getRecordStartText() {
        return "LOCUS";
    }

    @Override
    protected void processRecord(String s) throws Exception {
        updateTables(new Record(s));
    }


////////////////////////////////////////////////////////////////////////////////////////////
// private methods
//

    private void extractFeatureTableData(String file) throws IOException {
        Path dir = Paths.get(FileUtil.getPathPart(file));
        String tmpFilename = Files.createTempFile(dir, null, null).toString();

        extractTableInfo(file, tmpFilename);

        FileUtil.delete(file);
        FileUtil.moveFile(tmpFilename, file);
    }

    /**
     * Pares down data from the raw source file downloaded from NIH to strip out genome sequence data
     * (since all we're interested in is the metadata).  This saves disk space and simplifies downstream
     * processing.
     * @param srcFilename the source file from NIH
     * @param destFilename the destination file, which contains only relevant metadata
     * @throws IOException
     */
    private void extractTableInfo(String srcFilename, String destFilename) throws IOException {
        GZIPInputStream input = null;
        BufferedReader reader = null;
        GZIPOutputStream output = null;
        PrintWriter writer = null;

        try {
            input = new GZIPInputStream(new FileInputStream(srcFilename));
            reader = new BufferedReader(new InputStreamReader(input));

            output = new GZIPOutputStream(new FileOutputStream(destFilename));
            writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(output)));

            boolean inUsefulDataBlock = false;
            boolean isTranslateLine = false;
            boolean isVariation = false;

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("LOCUS")) {
                    inUsefulDataBlock = true;
                    isTranslateLine = false;

                } else if (line.startsWith("ORIGIN") || line.startsWith("CONTIG")) {
                    inUsefulDataBlock = false;
                }

                if (inUsefulDataBlock) {
                    if (line.matches("\\s+variation\\s+.+")) {
                        isVariation = true;
                    } else if (isVariation && line.contains("/db_xref=\"")) {
                        isVariation = false;
                    }

                    if (line.contains("/translation=\"")) {     // a large, frequent, un-useful sub-block of data
                        isTranslateLine = true;
                    } else if (isTranslateLine && line.endsWith("\"")) {
                        isTranslateLine = false;
                    }

                    if ( ! isVariation && ! isTranslateLine ) {
                        writer.println(line);
                    }
                }
            }

        } finally {
            try { if (writer != null) { writer.flush(); writer.close(); } } catch (Exception e) {}
            try { if (reader != null) reader.close(); } catch (Exception e) {}

            try { if (output != null) { output.flush(); output.close(); } } catch (Exception e) {}
            try { if (input != null) input.close(); } catch (Exception e) {}
        }
    }

    /**
     * Updates table-data buffers with information from the current record
     * @param r a {@link Record}
     * @throws IOException
     */
    private void updateTables(Record r) throws IOException {
        tBasic.addRecord(r.getPartitionKey(), r.getLocus(), r.getYear(), r.getMonth(), r.getVersion(),
                r.getGiNumber(), r.getDefinition());

        for (String keyword : r.getKeywords()) {
            tKeywords.addRecord(r.getPartitionKey(), r.getLocus(), keyword);
        }

        for (String author : r.getAuthors()) {
            tAuthors.addRecord(r.getPartitionKey(), r.getLocus(), author);
        }

        for (Journal j : r.getJournals()) {
            tJournals.addRecord(r.getPartitionKey(), r.getLocus(), j.getName(), j.getCitation(), j.getPmid());
        }

        for (DbXRef ref : r.getDbxrefs()) {
            tDbXRef.addRecord(r.getPartitionKey(), r.getLocus(), ref.getDatabaseName(), ref.getDatabaseId());
        }

        for (Map.Entry<String, Set<String>> entry : r.getFeatures().entrySet()) {
            for (String value : entry.getValue()) {
                String indexedValue = (value != null && value.length() > ANNOTATION_VALUE_INDEX_LEN) ?
                        value.substring(0, ANNOTATION_VALUE_INDEX_LEN) :
                        value;

                tAnnotations.addRecord(r.getPartitionKey(), r.getLocus(), entry.getKey(), indexedValue, value);
            }
        }
    }
}

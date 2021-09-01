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

package edu.uvm.ccts.common.db.parser;

import edu.uvm.ccts.common.util.FileUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * This class acts as a logical buffer between parsed records and the files into which those
 * records are stored.
 */
public class TableData {
    private static final Log log = LogFactory.getLog(TableData.class);
    private static final String FIELD_DELIM = "\t";
    private static final String LINE_DELIM = "\n";

    private static final int BYTE_THRESHOLD = 500000;

    private List<String> list = new ArrayList<String>();
    private int byteCount = 0;
    private String tempFilename;
    private String filename;

    /**
     * Creates a new TableData buffer.
     * @param tempDir the name of a directory into which temporary files will be written
     * @param outputDir the name of the target directory into which the finalized data will be written
     * @param filename the name of the file
     * @throws IOException
     */
    public TableData(String tempDir, String outputDir, String filename) throws IOException {
        this.tempFilename = tempDir + "/" + filename;
        this.filename = outputDir + "/" + filename;

        FileUtil.createDirectory(tempDir);
        FileUtil.createDirectory(outputDir);
    }

    public String getFilename() {
        return filename;
    }

    /**
     * Adds a new record to this table.  Records may consist of one or more Objects, each of which will be serialized
     * to string before being written to disk.  Serialization occurs simply by calling {@code toString()} on any Object
     * that is not already a {@link String}.  {@code null} values are permitted.
     * @param recordParts one or more field values, all of which together represents a single record
     * @throws IOException
     */
    public void addRecord(Object ... recordParts) throws IOException {
        if (recordParts != null && recordParts.length > 0 && anyNotNull(recordParts)) {
            String[] arr = new String[recordParts.length];
            for (int i = 0; i < recordParts.length; i ++) {
                if (recordParts[i] == null) {
                    arr[i] = "\\N";                         // null character

                } else if (recordParts[i] instanceof String) {
                    String s = (String) recordParts[i];
                    arr[i] = s.contains("\\") ? s.replaceAll("\\\\", "\\\\\\\\") : s;

                } else if (recordParts[i] instanceof Boolean) {
                    arr[i] = (Boolean) recordParts[i] ? "1" : "0";

                } else {
                    arr[i] = String.valueOf(recordParts[i]);
                }
            }

            add(StringUtils.join(arr, FIELD_DELIM));
        }
    }

    /**
     * Flushes the contents of the internal buffer to disk.  This function is called whenever the internal buffer
     * exceeds {@code BYTE_THRESHOLD} bytes.  All flushed data is written to a temporary file; {@code finalizeUpdates}
     * must be called to dump the contents of the temporary file to the master file.
     * @throws IOException
     */
    public void flush() throws IOException {
        OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFilename, true));

        try {
            for (String s : list) {
                output.write((s + LINE_DELIM).getBytes());
            }

        } finally {
            list.clear();
            byteCount = 0;
            try { output.flush(); } catch (Exception e) {}
            try { output.close(); } catch (Exception e) {}
        }
    }

    /**
     * Copies the contents of the temporary / working file to the authoritative / master file.  Note that data is not
     * written directly to the master file on calls to {@code flush} to prevent the corruption of the authoritative /
     * master file, should a processing error require the system to be restarted, and processing to be resumed from
     * where it left off.
     * @throws IOException
     */
    public void finalizeUpdates() throws IOException {
        try {
            FileUtil.append(tempFilename, filename);

        } catch (IOException e) {
            log.error("caught " + e.getClass().getName() + " finalizing updates to file '" + filename +
                    "' - the contents may have been corrupted!", e);
            throw e;

        } catch (RuntimeException e) {
            log.error("caught " + e.getClass().getName() + " finalizing updates to file '" + filename +
                    "' - the contents may have been corrupted!", e);
            throw e;

        } finally {
            FileUtil.delete(tempFilename);
        }
    }

//////////////////////////////////////////////////////////////////////////////////////////
// private methods
//

    /**
     * Adds a record to the internal buffer.  Will trigger a call to {@code flush} if the added record causes the
     * internal buffer to exceed {@code BYTE_THRESHOLD} bytes in size.
     * @param s
     * @throws IOException
     */
    private void add(String s) throws IOException {
        if (s != null) {
            list.add(s);
            byteCount += s.length();

            if (byteCount >= BYTE_THRESHOLD) {
                flush();
            }
        }
    }

    /**
     * @param parts an array of {@link Object}s
     * @return {@code true} if any array item is not {@code} null; {@code false} otherwise (if all array items are
     * {@code} null).
     */
    private boolean anyNotNull(Object[] parts) {
        for (Object part : parts) {
            if (part != null) return true;
        }
        return false;
    }
}

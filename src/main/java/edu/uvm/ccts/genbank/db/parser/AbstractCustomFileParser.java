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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by mstorer on 5/20/14.
 */
public abstract class AbstractCustomFileParser extends AbstractFileParser {
    private static final Log log = LogFactory.getLog(AbstractCustomFileParser.class);

    protected abstract String getRecordStartText();
    protected abstract void processRecord(String record) throws Exception;

    public AbstractCustomFileParser(int threadId) {
        super(threadId);
    }

    public void parse(String filename) throws Exception {
        InputStream input = null;
        BufferedReader reader = null;
        long lineNo = 0;
        long byteCount = 0;

        try {
            input = getInputStream(filename);
            reader = new BufferedReader(new InputStreamReader(input));

            String line;
            StringBuilder sb = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                lineNo++;

                if (line.startsWith(getRecordStartText()) && sb.length() > 0) {
                    processRecord(sb);
                    byteCount = 0;
                }

                sb.append(line).append("\n");

                byteCount += line.length() + 1;
            }

            processRecord(sb);
            flushBuffers();
            finalizeUpdates();

        } catch (Error e) {
            log.error("[" + threadId + "]  encountered " + e.getClass().getName() + " processing file '" + filename +
                    "' on or about line " + lineNo + ".  current record size: " + byteCount + " bytes.  message: " +
                    e.getMessage(), e);
            throw e;

        } catch (Exception e) {
            log.error("[" + threadId + "]  encountered " + e.getClass().getName() + " processing file '" + filename +
                    "' on or about line " + lineNo + ".  current record size: " + byteCount + " bytes.  message: " +
                    e.getMessage(), e);
            throw e;

        } finally {
            try { if (reader != null) reader.close(); } catch (Exception e) {}
            try { if (input != null) input.close(); } catch (Exception e) {}
        }
    }

    private void processRecord(StringBuilder sb) throws Exception {
        try {
            processRecord(sb.toString());

        } catch (Exception e) {
            log.error("error processing record:\n------------------------\n" +
                    sb.toString() + "\n------------------------\n");
            throw e;

        } finally {
            sb.setLength(0);
        }
    }
}

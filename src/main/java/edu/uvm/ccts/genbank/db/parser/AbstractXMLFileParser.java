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

import electric.xml.Document;
import electric.xml.Element;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by mstorer on 5/20/14.
 */
public abstract class AbstractXMLFileParser extends AbstractFileParser {
    private static final Log log = LogFactory.getLog(AbstractXMLFileParser.class);

    protected abstract String getRecordElementName();
    protected abstract void processRecord(Element record) throws Exception;

    public AbstractXMLFileParser(int threadId) {
        super(threadId);
    }

    @Override
    public void parse(String filename) throws Exception {
        InputStream input = null;
        BufferedReader reader = null;
        long lineNo = 0;
        long byteCount = 0;
        boolean recording = false;

        try {
            input = getInputStream(filename);
            reader = new BufferedReader(new InputStreamReader(input));

            String line;
            StringBuilder sb = new StringBuilder();
            Pattern start = Pattern.compile("^.*(<" + getRecordElementName() + "\\W.*)$");
            Pattern end = Pattern.compile("^(.*<\\/" + getRecordElementName() + ">).*$");

            // note : the following works fine when the start tag and end tag do not occur on the same line.

            while ((line = reader.readLine()) != null) {
                lineNo++;

                if (recording) {
                    Matcher m = end.matcher(line);
                    if (m.matches()) {
                        String s = m.group(1);
                        sb.append(s);
                        byteCount += s.length();
                        processRecord(sb);
                        byteCount = 0;
                        recording = false;

                    } else {
                        sb.append(line).append("\n");
                        byteCount += line.length() + 1;
                    }

                } else {
                    Matcher m = start.matcher(line);
                    if (m.matches()) {
                        String s = m.group(1);
                        sb.append(s).append("\n");
                        byteCount += s.length() + 1;
                        recording = true;
                    }
                }
            }

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
            Document doc = new Document(sb.toString());
            processRecord(doc.getElement(getRecordElementName()));

        } catch (Exception e) {
            log.error("error processing record:\n------------------------\n" +
                    sb.toString() + "\n------------------------\n");
            throw e;

        } finally {
            sb.setLength(0);
        }
    }
}

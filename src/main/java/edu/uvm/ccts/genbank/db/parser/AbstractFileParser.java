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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by mstorer on 5/19/14.
 */
public abstract class AbstractFileParser {
    protected final int threadId;

    public abstract void parse(String filename) throws Exception;

    protected abstract List<TableData> getTableDataList();
    protected abstract InputStream getInputStream(String filename) throws IOException;


    public AbstractFileParser(int threadId) {
        this.threadId = threadId;
    }

    /**
     * Flushes table-data buffers to disk (to the working, temporary file)
     * @throws IOException
     */
    protected void flushBuffers() throws IOException {
        for (TableData td : getTableDataList()) {
            td.flush();
        }
    }

    /**
     * Appends the contents of the working, temporary files that represent the parsed records into the authoritative,
     * master file that contains data for all records from all threads.  This should only be called after all records
     * have been parsed and no errors have been detected.
     * @throws IOException
     */
    protected void finalizeUpdates() throws IOException {
        for (TableData td : getTableDataList()) {
            td.finalizeUpdates();
        }
    }

}

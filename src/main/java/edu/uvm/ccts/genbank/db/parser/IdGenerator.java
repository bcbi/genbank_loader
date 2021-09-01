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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mstorer on 5/12/14.
 */
public class IdGenerator {
    private static IdGenerator gen = null;

    public static IdGenerator getInstance() {
        if (gen == null) gen = new IdGenerator();
        return gen;
    }


///////////////////////////////////////////////////////////////
// instance methods

    private Map<String, Integer> nextIdMap = new HashMap<String, Integer>();

    private IdGenerator() {}

    public void reset() {
        nextIdMap.clear();
    }

    public Integer nextId(String tableName) {
        synchronized(tableName.intern()) {
            Integer id = nextIdMap.get(tableName);
            if (id == null) id = 1;
            nextIdMap.put(tableName, id + 1);
            return id;
        }
    }
}

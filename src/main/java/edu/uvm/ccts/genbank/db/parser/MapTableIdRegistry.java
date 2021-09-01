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

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mstorer on 5/13/14.
 */
public class MapTableIdRegistry {
    private static MapTableIdRegistry registry = null;

    public static MapTableIdRegistry getInstance() {
        if (registry == null) registry = new MapTableIdRegistry();
        return registry;
    }


//////////////////////////////////////////////////////////////////////////////////
// instance methods
//

    private Map<String, Map<String, Integer>> tableKeyIdMap = new HashMap<String, Map<String, Integer>>();

    private MapTableIdRegistry() {}

    public void reset() {
        tableKeyIdMap.clear();
    }

    public boolean hasId(String tableName, String key) throws NoSuchAlgorithmException {
        synchronized(tableName.intern()) {
            Map<String, Integer> keyIdMap = tableKeyIdMap.get(tableName);

            return keyIdMap != null && keyIdMap.containsKey(key);
        }
    }

    public Integer generateId(String tableName, String key) throws NoSuchAlgorithmException {
        synchronized(tableName.intern()) {
            Map<String, Integer> keyIdMap = tableKeyIdMap.get(tableName);
            if (keyIdMap == null) {
                keyIdMap = new HashMap<String, Integer>();
                tableKeyIdMap.put(tableName, keyIdMap);
            }

            Integer id = keyIdMap.get(key);
            if (id == null) {
                id = IdGenerator.getInstance().nextId(tableName);
                keyIdMap.put(key, id);
            }

            return id;
        }
    }

    public Integer getId(String tableName, String key) throws NoSuchAlgorithmException {
        synchronized(tableName.intern()) {
            Map<String, Integer> keyIdMap = tableKeyIdMap.get(tableName);

            return keyIdMap != null ?
                    keyIdMap.get(key) :
                    null;
        }
    }
}

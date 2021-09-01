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

package edu.uvm.ccts.common.db;

import edu.uvm.ccts.common.model.Credentials;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mstorer on 11/4/14.
 */
public class DomainCredentialRegistry {
    private static DomainCredentialRegistry registry = null;

    public static DomainCredentialRegistry getInstance() {
        if (registry == null) {
            registry = new DomainCredentialRegistry();
        }
        return registry;
    }

//////////////////////////////////////////////////////////////////////////////////////

    private Map<String, Credentials> map = new HashMap<String, Credentials>();

    private DomainCredentialRegistry() {
    }

    public void register(String domain, Credentials credentials) {
        map.put(domain.toLowerCase(), credentials);
    }

    public boolean isRegistered(String domain) {
        return map.containsKey(domain.toLowerCase());
    }

    public Credentials getCredentials(String domain) {
        return map.get(domain.toLowerCase());
    }

    public void clear() {
        map.clear();
    }
}

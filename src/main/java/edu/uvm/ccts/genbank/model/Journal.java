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

package edu.uvm.ccts.genbank.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: mstorer
 * Date: 12/3/13
 * Time: 3:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class Journal {
    private String name;
    private String citation;
    private String pmid;
    private List<String> authors = new ArrayList<String>();

    public Journal(String name, String citation, String pmid, List<String> authors) {
        this.name = name;
        this.citation = citation;
        this.pmid = pmid;
        this.authors = authors;
    }

    public String getName() {
        return name;
    }

    public String getCitation() {
        return citation;
    }

    public String getPmid() {
        return pmid;
    }

    public List<String> getAuthors() {
        return authors != null ? authors : new ArrayList<String>();
    }
}

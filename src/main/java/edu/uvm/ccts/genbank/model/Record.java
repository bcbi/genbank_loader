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

import edu.uvm.ccts.genbank.exceptions.TagNotFoundException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: mstorer
 * Date: 12/3/13
 * Time: 11:34 AM
 * To change this template use File | Settings | File Templates.
 */
public class Record {
    private static final Log log = LogFactory.getLog(Record.class);

    private static final Pattern headerPattern = Pattern.compile("^([A-Z]+)\\s+(.+)");

    private static final String TAG_LOCUS = "LOCUS";
    private static final String TAG_DEFINITION = "DEFINITION";
    private static final String TAG_VERSION = "VERSION";
    private static final String TAG_KEYWORDS = "KEYWORDS";
    private static final String TAG_REFERENCE = "REFERENCE";
    private static final String TAG_AUTHORS = "AUTHORS";
    private static final String TAG_JOURNAL = "JOURNAL";
    private static final String TAG_PUBMED = "PUBMED";
    private static final String TAG_FEATURES = "FEATURES";

    private static final String FEATURE_DBXREF = "db_xref";

    // hacky, but simple work-around to annoying SimpleDateFormat issue with synchronization, or lack thereof -
    // - we just won't use it!
    private static final Map<String, Integer> monthMap = new HashMap<String, Integer>() {{
        put("JAN", 1);
        put("FEB", 2);
        put("MAR", 3);
        put("APR", 4);
        put("MAY", 5);
        put("JUN", 6);
        put("JUL", 7);
        put("AUG", 8);
        put("SEP", 9);
        put("OCT", 10);
        put("NOV", 11);
        put("DEC", 12);
    }};

    private int partitionKey;
    private String locus;
    private int month;
    private int year;
    private String definition;
    private String version;
    private String giNumber;

    private List<String> keywords = new ArrayList<String>();
    private List<Journal> journals = new ArrayList<Journal>();
    private Map<String, Set<String>> features = new LinkedHashMap<String, Set<String>>();
    private List<DbXRef> dbxrefs = new ArrayList<DbXRef>();

    public Record(String record) throws TagNotFoundException, ParseException, NoSuchAlgorithmException {
        Map<String, List<String>> map = splitIntoLogicalParts(record);

        populateLocus(map);
        populatePartitionKey();         // must occur after locus has been populated

        try {
            populateDate(map);
            populateDefinition(map);
            populateVersion(map);
            populateGiNumber(map);
            populateKeywords(map);
            populateJournals(map);
            populateFeatures(map);

            populateDbXRefs();          // must occur after features have been populated

        } catch (ParseException e) {
            log.error("encountered " + e.getClass().getName() + " processing record with locus = '" + locus + "' - " + e.getMessage(), e);
            throw e;

        } catch (TagNotFoundException e) {
            log.error("encountered " + e.getClass().getName() + " processing record with locus = '" + locus + "' - " + e.getMessage(), e);
            throw e;

        } catch (RuntimeException e) {
            log.error("encountered " + e.getClass().getName() + " processing record with locus = '" + locus + "' - " + e.getMessage(), e);
            throw e;
        }
    }

    public String getLocus() {
        return locus;
    }

    public int getPartitionKey() {
        return partitionKey;
    }

    public int getMonth() {
        return month;
    }

    public int getYear() {
        return year;
    }

    public String getDefinition() {
        return definition;
    }

    public String getVersion() {
        return version;
    }

    public String getGiNumber() {
        return giNumber;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    public List<Journal> getJournals() {
        return journals;
    }

    public List<String> getAuthors() {
        List<String> list = new ArrayList<String>();

        for (Journal j : journals) {
            for (String author : j.getAuthors()) {
                if ( ! list.contains(author) ) {
                    list.add(author);
                }
            }
        }

        return list;
    }

    public Map<String, Set<String>> getFeatures() {
        return features;
    }

    public List<DbXRef> getDbxrefs() {
        return dbxrefs;
    }


/////////////////////////////////////////////////////////////////////////////////////////////////////
// private methods
//

    private void populateLocus(Map<String, List<String>> map) throws TagNotFoundException {
        if ( ! map.containsKey(TAG_LOCUS) ) throw new TagNotFoundException(TAG_LOCUS);
        locus = map.get(TAG_LOCUS).get(0).split("\\s+")[0];
    }

    private void populatePartitionKey() throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(locus.getBytes());
        byte[] bytes = md.digest();
        ByteBuffer buf = ByteBuffer.wrap(Arrays.copyOfRange(bytes, bytes.length - 4, bytes.length));
        partitionKey = Math.abs(buf.getInt(0)) % 256;
    }

    private void populateDate(Map<String, List<String>> map) throws TagNotFoundException, ParseException {
        if ( ! map.containsKey(TAG_LOCUS) ) throw new TagNotFoundException(TAG_LOCUS);
        String[] parts = map.get(TAG_LOCUS).get(0).split("\\s+")[6].split("-");
        month = monthMap.get(parts[1]);
        year = Integer.parseInt(parts[2]);
    }

    private void populateDefinition(Map<String, List<String>> map) throws TagNotFoundException {
        if ( ! map.containsKey(TAG_DEFINITION) ) throw new TagNotFoundException(TAG_DEFINITION);
        definition = map.get(TAG_DEFINITION).get(0).replaceAll("\n", " ");
    }

    private void populateVersion(Map<String, List<String>> map) {
        if (map.containsKey(TAG_VERSION)) {
            String s = map.get(TAG_VERSION).get(0).split("\\s+")[0];
            version = s.substring(s.lastIndexOf('.') + 1);
        }
    }

    private void populateGiNumber(Map<String, List<String>> map) {
        if (map.containsKey(TAG_VERSION)) {
            giNumber = map.get(TAG_VERSION).get(0).split("\\s+")[1].substring(3);
        }
    }

    private void populateKeywords(Map<String, List<String>> map) {
        if (map.containsKey(TAG_KEYWORDS)) {
            String str = StringUtils.chop(map.get(TAG_KEYWORDS).get(0).replaceAll("\n", " ").trim());
            for (String s : str.split("\\s*;\\s*")) {
                if ( ! s.isEmpty() ) keywords.add(s);
            }
        }
    }

    private void populateJournals(Map<String, List<String>> map) {
        if (map.containsKey(TAG_REFERENCE)) {
            for (String ref : map.get(TAG_REFERENCE)) {
                Journal j = parseJournal(ref);
                if (j != null) journals.add(j);
            }
        }
    }

    private Journal parseJournal(String s) {
        Map<String, List<String>> m = splitIntoLogicalParts(s);

        if ( ! m.containsKey(TAG_JOURNAL) ) return null;

        String journalName = parseJournalName(m);
        String citation = parseCitation(m);
        String pmid = parsePMID(m);
        List<String> authors = parseAuthors(m);

        return new Journal(journalName, citation, pmid, authors);
    }

    private String parseJournalName(Map<String, List<String>> map) {
        String s = map.get(TAG_JOURNAL).get(0).replaceAll("\n", " ");
        List<String> list = new ArrayList<String>();

        if (s.toLowerCase().startsWith("submitted")) {
            return "Submitted";

        } else if (s.toLowerCase().startsWith("unpublished")) {
            return "Unpublished";

        } else {
            for (String word : s.split("\\s+")) {
                if (word.matches(".*\\d.*")) break;
                list.add(word);
            }

            return StringUtils.join(list, " ");
        }
    }

    private String parseCitation(Map<String, List<String>> map) {
        return map.get(TAG_JOURNAL).get(0).replaceAll("\n", " ");
    }

    private String parsePMID(Map<String, List<String>> map) {
        return map.containsKey(TAG_PUBMED) ?
                map.get(TAG_PUBMED).get(0) :
                null;
    }

    private List<String> parseAuthors(Map<String, List<String>> map) {
        if ( ! map.containsKey(TAG_AUTHORS) ) return null;

        String s = map.get(TAG_AUTHORS).get(0)
                .replaceAll("\n", " ")
                .replaceAll("\\s{2,}", " ");

        return Arrays.asList(s.split("(,|\\s+and)\\s+"));
    }

    private void populateFeatures(Map<String, List<String>> map) {
        if (map.containsKey(TAG_FEATURES)) {
            String[] lines = map.get(TAG_FEATURES).get(0).split("\n");
            int lineNo = 0;

            while (lineNo < lines.length) {
                String line = lines[lineNo++];
                if (line.startsWith("/")) {
                    int index = line.indexOf('=');
                    if (index >= 0) {
                        String key = line.substring(1, index);
                        String val = line.substring(index + 1);

                        if (val.startsWith("\"")) {
                            if (val.length() == 1) {                        // handle the odd-case in which a value
                                val += lines[lineNo++].trim();              // starts with a quote, but everything else
                            }                                               // comes on following lines

                            while ( ! val.endsWith("\"") ) {                // handle multiline values
                                val += " " + lines[lineNo++].trim();
                            }
                            val = val.substring(1, val.length() - 1);       // remove quotes
                        }

                        if ( ! features.containsKey(key) ) {
                            features.put(key, new LinkedHashSet<String>());
                        }

                        features.get(key).add(val);
                    }
                }
            }
        }
    }

    private void populateDbXRefs() {
        if (features.containsKey(FEATURE_DBXREF)) {
            for (String s : features.get(FEATURE_DBXREF)) {
                String[] parts = s.split(":");
                if (parts.length == 1) {
                    dbxrefs.add(new DbXRef(parts[0], null));

                } else if (parts.length == 2) {
                    dbxrefs.add(new DbXRef(parts[0], parts[1]));
                }
            }
            features.remove(FEATURE_DBXREF);
        }
    }

    private Map<String, List<String>> splitIntoLogicalParts(String record) {
        Map<String, List<String>> map = new HashMap<String, List<String>>();

        String key = null;
        List<String> buffer = new ArrayList<String>();

        for (String line : record.split("\n")) {
            Matcher m = headerPattern.matcher(line);
            if (m.matches()) {
                if (key != null) {
                    map.get(key).add(StringUtils.join(buffer, "\n"));
                }
                buffer.clear();

                key = m.group(1);
                if ( ! map.containsKey(key) ) {
                    map.put(key, new ArrayList<String>());
                }

                buffer.add(m.group(2).trim());

            } else {
                buffer.add(line.trim());
            }
        }

        // flush buffer
        if (key != null && buffer.size() > 0) {
            map.get(key).add(StringUtils.join(buffer, "\n"));
        }

        return map;
    }
}

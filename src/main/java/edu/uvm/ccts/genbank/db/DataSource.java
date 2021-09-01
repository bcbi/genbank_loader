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

import edu.uvm.ccts.common.exceptions.BadCredentialsException;
import edu.uvm.ccts.common.exceptions.ConfigurationException;
import edu.uvm.ccts.common.model.AbstractCredentialedObject;
import edu.uvm.ccts.common.model.Credentials;
import edu.uvm.ccts.common.util.PropertiesUtil;
import electric.xml.Element;
import electric.xml.Elements;
import electric.xml.XPath;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.awt.*;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.List;

/**
 * Created by mstorer on 7/23/13.
 */
public class DataSource extends AbstractCredentialedObject implements Serializable {
    private static final Log log = LogFactory.getLog(DataSource.class);

    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String DOMAIN = "domain";


    public static DataSource buildFromXml(Element xmlDataSource) throws ConfigurationException {
        return buildFromXml(null, xmlDataSource);
    }

    public static DataSource buildFromXml(String name, Element xmlDataSource) throws ConfigurationException {
        return buildFromXml(name, xmlDataSource, null);
    }

    public static DataSource buildFromXml(Element xmlDataSource, Frame frame) throws ConfigurationException {
        return buildFromXml(null, xmlDataSource, frame);
    }

    public static DataSource buildFromXml(String name, Element xmlDataSource, Frame frame) throws ConfigurationException {
        Properties info = new Properties();
        Elements xmlProperties = xmlDataSource.getElements(new XPath("properties/property"));
        while (xmlProperties.hasMoreElements()) {
            Element xmlProperty = xmlProperties.next();
            info.setProperty(xmlProperty.getAttribute("name"), xmlProperty.getAttribute("value"));
        }

        if (name == null) name = xmlDataSource.getAttribute("name");

        return new DataSource(name,
                xmlDataSource.getAttribute("driver"),
                xmlDataSource.getAttribute("url"),
                xmlDataSource.getAttribute("note"),
                info,
                frame);
    }

    private static boolean shutdown = false;
    private static final transient Map<String, Map<Long, Connection>> connMap = new HashMap<String, Map<Long, Connection>>();
    private static Thread maint = null;

    public static synchronized boolean startMaintenanceThread(final int countdownMax, final int periodInSeconds) {
        if (maint != null) return false;

        if (countdownMax <= 0)      throw new IllegalArgumentException("countdownMax must be > 0");
        if (periodInSeconds <= 0)   throw new IllegalArgumentException("periodInSeconds must be > 0");

        log.info("initializing maintenance thread with countdown=" + countdownMax + ", period=" + periodInSeconds + " seconds");

        maint = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    int countdown = countdownMax;
                    while (countdown >= 0) {
                        int remainingConnections = closeAbandonedConnections();

                        if (shutdown) {
                            if (remainingConnections == 0) {
                                break;

                            } else {
                                log.info("system has " + remainingConnections + " remaining open connections.  will try " +
                                        countdown + " more times to gracefully stop before pulling the plug.");

                                countdown --;
                            }
                        }

                        Thread.sleep(periodInSeconds * 1000);
                    }

                } catch (InterruptedException e) {
                    // handle silently

                } finally {
                    log.debug("maintenance thread terminated.");
                    closeAllConnections();
                }
            }

            private int closeAbandonedConnections() {
                synchronized(connMap) {
                    int count = 0;

                    List<Long> threadIds = new ArrayList<Long>();
                    for (Thread thread : Thread.getAllStackTraces().keySet()) {
                        threadIds.add(thread.getId());
                    }

                    for (Map.Entry<String, Map<Long, Connection>> e1 : connMap.entrySet()) {
                        String name = e1.getKey();

                        Iterator<Map.Entry<Long, Connection>> iter = e1.getValue().entrySet().iterator();
                        while (iter.hasNext()) {
                            Map.Entry<Long, Connection> e2 = iter.next();
                            Long threadId = e2.getKey();

                            if ( ! threadIds.contains(threadId) ) {
                                log.debug("killing connection for '" + name + "' on expired thread(" + threadId + ")");

                                try {
                                    Connection conn = e2.getValue();
                                    if ( ! conn.isClosed() ) {
                                        conn.close();
                                    }

                                } catch (Exception e) {
                                    // handle silently

                                } finally {
                                    iter.remove();
                                }

                            } else {
                                // this is an active thread - update active connections count
                                try {
                                    Connection conn = e2.getValue();
                                    if ( ! conn.isClosed() ) {
                                        count ++;
                                    }

                                } catch (Exception e) {
                                    // handle silently
                                }
                            }
                        }
                    }

                    return count;
                }
            }

            private void closeAllConnections() {
                synchronized(connMap) {
                    for (Map.Entry<String, Map<Long, Connection>> e1 : connMap.entrySet()) {
                        String name = e1.getKey();

                        Iterator<Map.Entry<Long, Connection>> iter = e1.getValue().entrySet().iterator();
                        while (iter.hasNext()) {
                            Map.Entry<Long, Connection> e2 = iter.next();
                            Long threadId = e2.getKey();
                            Connection conn = e2.getValue();

                            try {
                                if (conn != null && ! conn.isClosed()) {
                                    log.info("closing connection '" + name + "' on thread(" + threadId + ")");
                                    conn.close();
                                }

                            } catch (SQLException e) {
                                log.error("caught " + e.getClass().getName() + " closing connection for '" + name + "' on thread(" + threadId + ") - " +
                                        e.getMessage(), e);

                            } finally {
                                iter.remove();
                            }
                        }
                    }
                }

                log.info("all data source connections have been closed.");
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("shutting down data source connections...");
                shutdown = true;

                try {
                    maint.join();

                } catch (InterruptedException e) {
                    log.error("caught " + e.getClass().getName() + " joining maintenance thread - " + e.getMessage(), e);
                }
            }
        });

        maint.start();

        return true;
    }



//////////////////////////////////////////////////////////////////////////////////////////////
// instance methods and vars
//

    private String name;
    private String driver;
    private String url;
    private Properties info;

    public DataSource(String name, String driver, String url, Properties info) throws ConfigurationException {
        this(name, driver, url, null, info);
    }

    public DataSource(String name, String driver, String url, String note, Properties info) throws ConfigurationException {
        this(name, driver, url, note, info, null);
    }

    public DataSource(String name, String driver, String url, String note, Properties info, Frame frame)
            throws ConfigurationException {

        this.name = name;
        this.driver = driver;

        int qmPos = url.indexOf('?');
        if (qmPos >= 0) {
            this.url = url.substring(0, qmPos);
            this.info = info;

            // add URL properties to info unless the URL property is already specified
            for (String s : url.substring(qmPos + 1).split("&")) {
                if (s.indexOf('=') < 0) continue;
                String[] kvPair = s.split("=");
                if ( ! this.info.containsKey(kvPair[0]) ) {
                    if (kvPair.length == 1) this.info.setProperty(kvPair[0], "");
                    else                    this.info.setProperty(kvPair[0], kvPair[1]);
                }
            }

        } else {
            this.url = url;
            this.info = info;
        }

        loadDriver(driver);
        ensureCredentials(note, frame);
    }

    public String getName() {
        return name;
    }

    public String getDriver() {
        return driver;
    }

    public String getUrl() {
        return url;
    }

    public Properties getInfo() {
        return info;
    }

    public Connection getConnection() throws SQLException {
        Connection conn;

        Long threadId = Thread.currentThread().getId();
        synchronized(connMap) {
            Map<Long, Connection> map = connMap.get(name);
            if (map == null) {
                map = new HashMap<Long, Connection>();
                connMap.put(name, map);
            }

            conn = map.get(threadId);
            if (conn == null || conn.isClosed()) {
                log.info("establishing connection to '" + name + "' on thread(" + threadId + ")");
                conn = DriverManager.getConnection(url, info);
                map.put(threadId, conn);
            }
        }

        return conn;
    }

    public void close() {
        Long threadId = Thread.currentThread().getId();
        synchronized(connMap) {
            Map<Long, Connection> map = connMap.get(name);
            if (map != null) {
                try {
                    Connection conn = map.get(threadId);

                    if (conn != null && ! conn.isClosed()) {
                        log.info("closing connection '" + name + "' on thread(" + threadId + ")");
                        conn.close();
                    }

                } catch (SQLException e) {
                    log.error("caught " + e.getClass().getName() + " closing connection for '" + name + "' on thread(" + threadId + ") - " +
                            e.getMessage(), e);

                } finally {
                    map.remove(threadId);
                }
            }
        }
    }

    // used in [mlm-engine] ManagementImpl to ensure the driver is loaded prior to use
    public void loadDriver() throws ConfigurationException {
        loadDriver(driver);
    }


//////////////////////////////////////////////////////////////////////////////////////////////////////
// private methods
//


    @Override
    protected void ensureCredentials(String note, Frame frame) throws ConfigurationException {
        boolean fallback = true;
        String domain = getDomain();

        if (domain != null) {
            DomainCredentialRegistry dcr = DomainCredentialRegistry.getInstance();

            if (dcr.isRegistered(domain)) {
                Credentials cred = dcr.getCredentials(domain);

                String user = getUser();
                if (user == null || user.equalsIgnoreCase(cred.getUser())) {
                    Properties testProps = PropertiesUtil.copy(info);
                    testProps.setProperty(USER, cred.getUser());
                    testProps.setProperty(PASSWORD, cred.getPassword());

                    try {
                        testCredentials(testProps);

                        fallback = false;

                        setUser(cred.getUser());
                        setPassword(cred.getPassword());

                    } catch (BadCredentialsException e) {
                        // handle silently
                    }
                }
            }
        }

        if (fallback) {
            super.ensureCredentials(note, frame);

            if (domain != null) {
                DomainCredentialRegistry.getInstance().register(domain, new Credentials(getUser(), getPassword()));
            }
        }
    }

    @Override
    protected void testCredentials() throws BadCredentialsException, ConfigurationException {
        testCredentials(info);
    }

    private void testCredentials(Properties props) throws BadCredentialsException, ConfigurationException {
        Connection c = null;
        try {
            c = DriverManager.getConnection(url, props);

        } catch (SQLException e) {
            throw new BadCredentialsException(e.getMessage(), e);

        } catch (Exception e) {
            throw new ConfigurationException(e.getMessage(), e);

        } finally {
            try { if (c != null) c.close(); } catch (Exception e) {}
        }
    }

    private void loadDriver(String driver) throws ConfigurationException {
        try {
            Class.forName(driver).newInstance();

        } catch (Exception e) {
            throw new ConfigurationException("invalid driver: " + driver, e);
        }
    }

    @Override
    protected String getCredentialType() {
        return "data source";
    }

    @Override
    protected String getCredentialTarget() {
        return name;
    }

    @Override
    protected String getUser() {
        return info.getProperty(USER);
    }

    @Override
    protected String getPassword() {
        return info.getProperty(PASSWORD);
    }

    private String getDomain() {
        return info.getProperty(DOMAIN);
    }

    @Override
    protected void setUser(String user) {
        if (user == null) info.remove(USER);
        else              info.setProperty(USER, user);
    }

    @Override
    protected void setPassword(String password) {
        if (password == null) info.remove(PASSWORD);
        else                  info.setProperty(PASSWORD, password);
    }
}

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

package edu.uvm.ccts.genbank;

import edu.uvm.ccts.common.db.DataSource;
import edu.uvm.ccts.common.util.TimeUtil;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;
import java.util.Properties;

/**
 * This is the main entry-point class, and is responsible for collecting parameters from
 * the user and passes control to {@link MetaGenbankLoader}, which handles the next level
 * of execution.
 */
public class Load {
    private static final Log log = LogFactory.getLog(Load.class);

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    public static void main(String[] args) {
        System.out.println("GenBank Loader");
        System.out.println("--------------");
        System.out.println("Copyright 2015 The University of Vermont and State Agricultural College.  All rights reserved.\n");

        if (args.length == 0) {
            showHelp();
            System.exit(0);
        }

        try {
            CommandLineParser parser = new BasicParser();
            CommandLine line = parser.parse(getCLIOptions(), args);

            MetaGenbankLoader loader = new MetaGenbankLoader();

            long start = System.currentTimeMillis();
            log.info("process started at " + new Date());

            if (line.hasOption("prepare")) {
                loader.prepare();
                log.info("preparing GenBank files finished at " + new Date() + " (took " +
                        TimeUtil.formatMsToHMS(System.currentTimeMillis() - start) + ").");

            } else if (line.hasOption("load")) {
                String host = line.hasOption("host") ? line.getOptionValue("host") : "localhost";
                String db = line.hasOption("db") ? line.getOptionValue("db") : "genbank";
                String user = line.hasOption("user") ? line.getOptionValue("user") : "genbank";
                String pass = line.hasOption("pass") ? line.getOptionValue("pass") : "genbank";

                String url = "jdbc:mysql://" + host + "/" + db;

                Properties properties = new Properties();
                properties.put("user", user);
                properties.put("password", pass);

                DataSource dataSource = new DataSource("genbank", JDBC_DRIVER, url, properties);

                String releaseNo = line.getOptionValue("load");
                loader.populateDatabase(dataSource);

                log.info("populating database with release '" + releaseNo + "' finished at " + new Date() + " (took " +
                        TimeUtil.formatMsToHMS(System.currentTimeMillis() - start) + ").");
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    /**
     * Display primary system help - not much to it, really
     */
    private static void showHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        formatter.printHelp("genbank-loader", getCLIOptions(), true);
    }

    private static Options getCLIOptions() {
        Options options = new Options();

        options.addOption(OptionBuilder.hasArg()
                .withLongOpt("host")
                .withArgName("string")
                .withDescription("the database host (default 'localhost')")
                .create('h'));
        options.addOption(OptionBuilder.hasArg()
                .withLongOpt("db")
                .withArgName("string")
                .withDescription("the database name (default 'genbank')")
                .create('d'));
        options.addOption(OptionBuilder.hasArg()
                .withLongOpt("user")
                .withArgName("string")
                .withDescription("the database user name (default 'genbank')")
                .create('u'));
        options.addOption(OptionBuilder.hasArg()
                .withLongOpt("pass")
                .withArgName("string")
                .withDescription("the database user password (default 'genbank')")
                .create('p'));

        OptionGroup group = new OptionGroup();
        group.addOption(OptionBuilder.withLongOpt("prepare")
                .withDescription("only prepare database files for import")
                .create());
        group.addOption(OptionBuilder.withLongOpt("load")
                .withDescription("only load prepared files into the target database")
                .create());
        group.setRequired(true);
        options.addOptionGroup(group);

        return options;
    }
}

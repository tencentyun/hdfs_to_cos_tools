package com.qcloud.hdfs_to_cos;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine cli = null;
        try {

            cli = parser.parse(OptionsArgsName.getAllSupportOption(), args);

            if (cli.hasOption(OptionsArgsName.HELP)) {
                OptionsArgsName.printHelpOption();
                return;
            }
        } catch (ParseException exp) {
            System.err.println("Parsing Argument failed. Reason: " + exp.getMessage());
            OptionsArgsName.printHelpOption();
            return;
        }

        ConfigReader configReader = new ConfigReader(cli);
        if (!configReader.isInitConfigFlag()) {
            System.err.println(configReader.getInitErrMsg());
            return;
        }
        Statistics.instance.start();
        HdfsToCos hdfsToCos = new HdfsToCos(configReader);
        hdfsToCos.run();
        Statistics.instance.printStatics();
    }
}

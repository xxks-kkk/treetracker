package org.zhu45.treetracker.benchmark.codegen;

import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.zhu45.treetracker.benchmark.codegen.GenerateSSBSetupScripts.Configuration.newConfiguration;

/**
 * We want to automatically generate SSB setup scripts to eliminate any manual
 * modification.
 */
public class GenerateSSBSetupScripts
{
    static class Configuration
    {
        String projectRoot = "/home/zeyuanhu/projects/treetracker2";
        String scaleFactor = "1";

        static Configuration newConfiguration()
        {
            return new Configuration();
        }
    }

    private GenerateSSBSetupScripts() {}

    private static HashMap<String, String> intoContext(Configuration configuration)
    {
        HashMap<String, String> context = new HashMap<>();
        context.put("projectRoot", configuration.projectRoot);
        context.put("scaleFactor", configuration.scaleFactor);
        return context;
    }

    private static void writeSetupSSB(Configuration configuration)
            throws IOException
    {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource("codegen/ssb/setup/setup-ssb.sht"), StandardCharsets.UTF_8);
        String output = jinjava.render(template, intoContext(configuration));
        Path outputPath = Paths.get(Resources.getResource("setup-ssb.sh").getPath()).getParent();
        outputPath = Paths.get(outputPath.getParent().getParent().toString(), "src", "main", "resources", "setup-ssb.sh");
        System.out.println("Write to: " + outputPath);
        FileUtils.writeStringToFile(new File(outputPath.toString()), output, StandardCharsets.UTF_8);
    }

    private static void writeSSBSQL(Configuration configuration)
            throws IOException
    {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource("codegen/ssb/setup/ssb.sqlt"), StandardCharsets.UTF_8);
        String output = jinjava.render(template, intoContext(configuration));
        Path outputPath = Paths.get(Resources.getResource("setup-ssb.sh").getPath()).getParent();
        outputPath = Paths.get(outputPath.getParent().getParent().toString(), "src", "main", "resources", "ssb.sql");
        System.out.println("Write to: " + outputPath);
        FileUtils.writeStringToFile(new File(outputPath.toString()), output, StandardCharsets.UTF_8);
    }

    private static void writeSSBAbbreviatedSQL(Configuration configuration)
            throws IOException
    {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource("codegen/ssb/setup/ssb-abbreviated.sqlt"), StandardCharsets.UTF_8);
        String output = jinjava.render(template, intoContext(configuration));
        Path outputPath = Paths.get(Resources.getResource("setup-ssb.sh").getPath()).getParent();
        outputPath = Paths.get(outputPath.getParent().getParent().toString(), "src", "main", "resources", "ssb-abbreviated.sql");
        System.out.println("Write to: " + outputPath);
        FileUtils.writeStringToFile(new File(outputPath.toString()), output, StandardCharsets.UTF_8);
    }

    private static void writeSSBAbbreviatedDuckDBSQL(Configuration configuration)
            throws IOException
    {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource("codegen/ssb/setup/ssb-abbreviated-duckdb.sqlt"), StandardCharsets.UTF_8);
        String output = jinjava.render(template, intoContext(configuration));
        Path outputPath = Paths.get(Resources.getResource("setup-ssb.sh").getPath()).getParent();
        outputPath = Paths.get(outputPath.getParent().getParent().toString(), "src", "main", "resources", "ssb-abbreviated-duckdb.sql");
        System.out.println("Write to: " + outputPath);
        FileUtils.writeStringToFile(new File(outputPath.toString()), output, StandardCharsets.UTF_8);
    }

    private static void writeSSBDuckDBSQL(Configuration configuration)
            throws IOException
    {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource("codegen/ssb/setup/ssb-duckdb.sqlt"), StandardCharsets.UTF_8);
        String output = jinjava.render(template, intoContext(configuration));
        Path outputPath = Paths.get(Resources.getResource("setup-ssb.sh").getPath()).getParent();
        outputPath = Paths.get(outputPath.getParent().getParent().toString(), "src", "main", "resources", "ssb-duckdb.sql");
        System.out.println("Write to: " + outputPath);
        FileUtils.writeStringToFile(new File(outputPath.toString()), output, StandardCharsets.UTF_8);
    }

    private static void writeSSBOriginalSQL(Configuration configuration)
            throws IOException
    {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource("codegen/ssb/setup/ssb-original.sqlt"), StandardCharsets.UTF_8);
        String output = jinjava.render(template, intoContext(configuration));
        Path outputPath = Paths.get(Resources.getResource("setup-ssb.sh").getPath()).getParent();
        outputPath = Paths.get(outputPath.getParent().getParent().toString(), "src", "main", "resources", "ssb-original.sql");
        System.out.println("Write to: " + outputPath);
        FileUtils.writeStringToFile(new File(outputPath.toString()), output, StandardCharsets.UTF_8);
    }

    private static void writeDuckDBSetup(Configuration configuration)
            throws IOException
    {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource("codegen/ssb/setup/duckdb-setup.sht"), StandardCharsets.UTF_8);
        String output = jinjava.render(template, intoContext(configuration));
        Path outputPath = Paths.get(Resources.getResource("setup-ssb.sh").getPath()).getParent();
        outputPath = Paths.get(outputPath.getParent().getParent().toString(), "src", "main", "resources", "ssb", "duckdb-setup.sh");
        System.out.println("Write to: " + outputPath);
        FileUtils.writeStringToFile(new File(outputPath.toString()), output, StandardCharsets.UTF_8);
    }

    private static void writeSetup(Configuration configuration)
            throws IOException
    {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource("codegen/ssb/setup/setup.sht"), StandardCharsets.UTF_8);
        String output = jinjava.render(template, intoContext(configuration));
        Path outputPath = Paths.get(Resources.getResource("setup-ssb.sh").getPath()).getParent();
        outputPath = Paths.get(outputPath.getParent().getParent().toString(), "src", "main", "resources", "ssb", "setup.sh");
        System.out.println("Write to: " + outputPath);
        FileUtils.writeStringToFile(new File(outputPath.toString()), output, StandardCharsets.UTF_8);
    }

    private static void run()
            throws IOException
    {
        Configuration configuration = newConfiguration();
        writeSetupSSB(configuration);
        writeSSBSQL(configuration);
        writeSSBAbbreviatedSQL(configuration);
        writeSSBAbbreviatedDuckDBSQL(configuration);
        writeSSBDuckDBSQL(configuration);
        writeSSBOriginalSQL(configuration);
        writeDuckDBSetup(configuration);
        writeSetup(configuration);
    }

    public static void main(String[] args)
            throws IOException
    {
        run();
    }
}

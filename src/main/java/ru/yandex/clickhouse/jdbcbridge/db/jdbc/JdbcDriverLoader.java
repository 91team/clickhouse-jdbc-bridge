package ru.yandex.clickhouse.jdbcbridge.db.jdbc;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * This class performs registration of drivers from list of JAR's in external directory
 * If a directory is valid, then all the current drivers are unloaded, and only
 * drivers, provided in directory are loaded
 * Created by krash on 26.09.18.
 */
@Data
@Slf4j
public class JdbcDriverLoader {

    public static void load(Path driverDirectory) throws SQLException, MalformedURLException {

        // if not specified, we'll use drivers from system classpath
        if (null == driverDirectory) {
            log.info("No driver directory specified");
            return;
        }

        Iterator<String> driverNames = Arrays.asList(
                "com.mysql.jdbc.Driver",
                "org.postgresql.Driver",
                "oracle.jdbc.driver.OracleDriver").iterator();

        while (driverNames.hasNext()) {
            try {
                Class.forName(driverNames.next());
            } catch (ClassNotFoundException e) {
                log.error("ERROR LOAD driver: " + e.getMessage());
            }
        }

        log.info("Looking for driver files in {}", driverDirectory);

        File sourceDir = driverDirectory.toFile();
        if (!sourceDir.exists()) {
            throw new IllegalArgumentException("Directory with drivers '" + sourceDir + "' does not exists");
        } else if (!sourceDir.isDirectory()) {
            throw new IllegalArgumentException("Given driver path '" + sourceDir + "' is not a director");
        }

        File[] driverList = sourceDir.listFiles(file -> file.isFile() && file.getName().endsWith(".jar"));
        if (null == driverList) {
            driverList = new File[0];
        }
        log.info("Found {} JAR file", driverList.length);

        for (File file : driverList) {
            log.info("Looking for driver in file {}", file);
            URLClassLoader ucl = new URLClassLoader(new URL[]{file.toURI().toURL()}, null);
            Iterator<Driver> foundDrivers = ServiceLoader.load(Driver.class, ucl).iterator();
            boolean found = false;
            while (foundDrivers.hasNext()) {
                Driver jarDriver = foundDrivers.next();
                DriverManager.registerDriver(new DriverWrapper(jarDriver));
                found = true;
                log.info("Registered driver {}", jarDriver.getClass().getName());
            }
            if (!found) {
                log.error("Failed to find JDBC driver in file {}", file);
            }
        }
    }

}

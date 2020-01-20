package com.scalar.db.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DatabaseConfig {
  private final Properties props;
  private List<String> contactPoints;
  private int contactPort;
  private String username;
  private String password;
  public static final String PREFIX = "scalar.database.";
  public static final String CONTACT_POINTS = PREFIX + "contact_points";
  public static final String CONTACT_PORT = PREFIX + "contact_port";
  public static final String USERNAME = PREFIX + "username";
  public static final String PASSWORD = PREFIX + "password";

  public DatabaseConfig(File propertiesFile) throws IOException {
    this(new FileInputStream(propertiesFile));
  }

  public DatabaseConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public DatabaseConfig(Properties properties) {
    props = new Properties(properties);
    load();
  }

  private void load() {
    checkNotNull(props.getProperty(CONTACT_POINTS));
    checkNotNull(props.getProperty(USERNAME));
    checkNotNull(props.getProperty(PASSWORD));

    contactPoints = Arrays.asList(props.getProperty(CONTACT_POINTS).split(","));
    if (props.getProperty(CONTACT_PORT) == null) {
      contactPort = 0;
    } else {
      contactPort = Integer.parseInt(props.getProperty(CONTACT_PORT));
      checkArgument(contactPort > 0);
    }
    username = props.getProperty(USERNAME);
    password = props.getProperty(PASSWORD);
  }

  public List<String> getContactPoints() {
    return contactPoints;
  }

  public int getContactPort() {
    return contactPort;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}

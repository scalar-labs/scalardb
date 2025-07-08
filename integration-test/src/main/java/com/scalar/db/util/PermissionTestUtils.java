package com.scalar.db.util;

public interface PermissionTestUtils {

  void createNormalUser(String userName, String password);

  void dropNormalUser(String userName);

  void grantRequiredPermission(String userName);

  void close();
}

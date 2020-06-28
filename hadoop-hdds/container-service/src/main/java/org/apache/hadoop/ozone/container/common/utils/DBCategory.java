package org.apache.hadoop.ozone.container.common.utils;

public class DBCategory {
  private String dbPath;
  private String categoryInDB;

  public DBCategory(String dbPath, String categoryInDB) {
    this.dbPath = dbPath;
    this.categoryInDB = categoryInDB;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getCategoryInDB() {
    return categoryInDB;
  }
}

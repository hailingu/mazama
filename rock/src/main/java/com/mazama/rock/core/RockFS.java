package com.mazama.rock.core;

/**
 * Rock File System
 *
 * @version 1.0
 */
public abstract class RockFS {

  String fileSystem = "abstract_rock_fs";

  /**
   * Get the file system
   */
  protected abstract String getFileSystem();
}

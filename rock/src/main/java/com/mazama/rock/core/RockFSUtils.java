package com.mazama.rock.core;

import com.mazama.rock.fs.hive.HiveFSUtils;
import java.util.Objects;

public class RockFSUtils {

  /**
   * Rock File System Type
   */
  public enum RockFSType {
    HIVE, Local
  }

  /**
   * Create a new file system or connect to a existing file system
   *
   * @param fsType file system type
   * @param args   file system arguments
   * @return a {@link RockFS} object
   */
  public static RockFS mkXFS(String fsType, String... args) {
    return null;
  }

  /**
   * Mount a file system
   *
   * @param rockFSType file system type
   * @param mountPoint mount point
   * @param args       file system arguments
   * @return a {@link RockFS} object
   */
  public static RockFS mount(RockFSType rockFSType, String mountPoint, String... args) {
    if (Objects.requireNonNull(rockFSType) == RockFSType.HIVE) {
      return HiveFSUtils.mount(mountPoint, args);
    }

    if (Objects.requireNonNull(rockFSType) == RockFSType.Local) {
      return null;
    }

    return null;
  }
}

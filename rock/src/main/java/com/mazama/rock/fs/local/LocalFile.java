package com.mazama.rock.fs.local;

import com.mazama.rock.core.RockFile;

public class LocalFile extends RockFile {

  @Override
  protected RockFile open(String... fileArgs) throws Exception {
    return null;
  }

  @Override
  protected int close(RockFile file) throws Exception {
    return 0;
  }

  @Override
  protected RockFile create(String path) throws Exception {
    return null;
  }

  @Override
  protected int delete(String path) throws Exception {
    return 0;
  }

  @Override
  protected boolean exists(String path) throws Exception {
    return false;
  }

  @Override
  protected int copy(RockFile src, RockFile dest) throws Exception {
    return 0;
  }
}

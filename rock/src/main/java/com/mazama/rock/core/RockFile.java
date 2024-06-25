package com.mazama.rock.core;

public abstract class RockFile {

    /**
     * Open a file
     *
     * @param fileArgs file arguments
     * @return {@link RockFile}
     */
    protected abstract RockFile open(String... fileArgs) throws Exception;

    /**
     * Close a file
     *
     * @param file a {@link RockFile} object
     */
    protected abstract int close(RockFile file) throws Exception;

    /**
     * Create a file
     *
     * @param path file path
     * @return {@link RockFile}
     */
    protected abstract RockFile create(String path) throws Exception;

    /**
     * Delete a file
     *
     * @param path file path
     */
    protected abstract int delete(String path) throws Exception;

    /**
     * Check if a file exists
     *
     * @param path file path
     */
    protected abstract boolean exists(String path) throws Exception;

    /**
     * Copy a file
     *
     * @param src  source file
     * @param dest destination file
     */
    protected abstract int copy(RockFile src, RockFile dest) throws Exception;

}

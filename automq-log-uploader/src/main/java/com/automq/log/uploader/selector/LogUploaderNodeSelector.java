package com.automq.log.uploader.selector;

/**
 * Determines whether the current node should act as the primary S3 log uploader.
 */
public interface LogUploaderNodeSelector {

    /**
     * @return {@code true} if the current node should upload and clean up logs in S3.
     */
    boolean isPrimaryUploader();

    /**
     * Creates a selector with a static boolean decision.
     *
     * @param primary whether this node should be primary
     * @return selector returning the static decision
     */
    static LogUploaderNodeSelector staticSelector(boolean primary) {
        return () -> primary;
    }
}

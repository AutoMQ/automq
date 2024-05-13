/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.utils;


import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class PingS3Helper {
    private static final Logger LOGGER = LoggerFactory.getLogger(PingS3Helper.class);
    private S3Operator s3Operator;
    private String endpoint;
    private String region;
    private String bucket;
    private boolean isForcePathStyle;
    private List<AwsCredentialsProvider> credentialsProviders;
    private boolean tagging;
    private final boolean needPrintToConsole;

    private PingS3Helper(Builder builder) {
        this.needPrintToConsole = builder.needPrintToConsole;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean needPrintToConsole;
        private String endpoint;
        private String region;
        private String bucket;
        private boolean isForcePathStyle;
        private List<AwsCredentialsProvider> credentialsProviders;
        private boolean tagging;

        public Builder needPrintToConsole(boolean needPrintToConsole) {
            this.needPrintToConsole = needPrintToConsole;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder isForcePathStyle(boolean isForcePathStyle) {
            this.isForcePathStyle = isForcePathStyle;
            return this;
        }

        public Builder credentialsProviders(List<AwsCredentialsProvider> credentialsProviders) {
            this.credentialsProviders = credentialsProviders;
            return this;
        }

        public Builder tagging(boolean tagging) {
            this.tagging = tagging;
            return this;
        }

        public PingS3Helper build() {
            PingS3Helper pingS3Helper = new PingS3Helper(this);
            pingS3Helper.endpoint = this.endpoint;
            pingS3Helper.region = this.region;
            pingS3Helper.bucket = this.bucket;
            pingS3Helper.isForcePathStyle = this.isForcePathStyle;
            pingS3Helper.credentialsProviders = this.credentialsProviders;
            pingS3Helper.tagging = this.tagging;
            return pingS3Helper;
        }
    }

    @Override
    public String toString() {
        return "s3 parameters{" +
            "endpoint='" + endpoint + '\'' +
            ", region='" + region + '\'' +
            ", bucket='" + bucket + '\'' +
            ", isForcePathStyle=" + isForcePathStyle +
            ", credentialsProviders=" + credentialsProviders +
            ", tagging=" + tagging +
            '}';
    }

    public void pingS3() {
        try {
            s3Operator = DefaultS3Operator.builder()
                .endpoint(endpoint)
                .region(region)
                .bucket(bucket)
                .forcePathStyle(isForcePathStyle)
                .credentialsProviders(credentialsProviders)
                .tagging(tagging)
                .checkS3ApiModel(true)
                .build();
        } catch (Exception e) {
            handleException(e, "Delete objects");
        }
        try {
            checkSimpleObjOperation();
            checkMultipartUploadOperation();
            checkUploadPartCopy().join();
            checkDeleteObjs().join();
        } finally {
            s3Operator.close();
        }
    }

    private void checkSimpleObjOperation() {
        byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
        String path = String.format("check_simple_obj_available/%d", System.nanoTime());

        CompletableFuture<Void> future = checkWrite(path, Unpooled.wrappedBuffer(content))
            .thenCompose(v -> checkRead(path, Unpooled.wrappedBuffer(content)))
            .thenCompose(v -> checkDelete(path));

        future.join();
    }

    private void checkMultipartUploadOperation() {
        String path = String.format("check_multipart_obj_available/%d", System.nanoTime());
        CompletableFuture<Void> future = checkCreateMultipartUpload(path)
            .thenCompose(respUploadId -> checkUploadPart(path, respUploadId)
                .thenApply(completedPart -> new AbstractMap.SimpleEntry<>(respUploadId, completedPart)))
            .thenCompose(entry ->
                checkCompleteMultipartUpload(path, entry.getKey(), entry.getValue())
            ).thenRun(() -> s3Operator.delete(path))
            .exceptionally(ex -> {
                handleException(ex, "Delete object");
                return null;
            });

        future.join();
    }

    private CompletableFuture<Void> checkWrite(String path, ByteBuf data) {
        return s3Operator.write(path, data).thenRun(() -> {
            LOGGER.info("Successfully write object to s3");
            printOperationStatus("Write object", true);
        }).exceptionally(ex -> handleException(ex, "Write object"));
    }

    private CompletableFuture<Void> checkRead(String path, ByteBuf data) {
        return s3Operator.rangeRead(path, 0, data.readableBytes()).thenAccept(buf -> {

            if (data.equals(buf)) {
                LOGGER.info("Successfully rangeRead object");
                printOperationStatus("RangeRead object", true);
                buf.release();
            } else {
                String exceptionMsg = "Failed to rangeRead object. The read data is empty/wrong";
                handleErrorResponse(exceptionMsg, "RangeRead object");
            }
        }).exceptionally(this::handleRangeReadException);
    }

    private CompletableFuture<Void> checkDelete(String path) {
        return s3Operator.delete(path).thenRun(() -> {
            LOGGER.info("Successfully delete object to s3");
            printOperationStatus("Delete object", true);
        }).exceptionally(ex -> handleException(ex, "Delete object"));
    }

    private CompletableFuture<String> checkCreateMultipartUpload(String path) {
        return s3Operator.createMultipartUpload(path)
            .thenApply(respUploadId -> {
                if (respUploadId != null && !respUploadId.isEmpty()) {
                    printOperationStatus("CreateMultipartUpload", true);
                    LOGGER.info("Successfully createMultipartUpload, uploadId: {}", respUploadId);
                } else {
                    String exceptionMsg = "Failed to createMultipartUpload, uploadId is empty";
                    handleErrorResponse(exceptionMsg, "CreateMultipartUpload");
                }
                return respUploadId;
            })
            .exceptionally(ex -> {
                handleException(ex, "CreateMultipartUpload");
                return null;
            });
    }

    private CompletableFuture<CompletedPart> checkUploadPart(String path, String uploadId) {
        byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
        return s3Operator.uploadPart(path, uploadId, 1, Unpooled.wrappedBuffer(content))
            .thenApply(completedPart -> {
                if (completedPart.eTag() != null && !completedPart.eTag().isEmpty() && completedPart.partNumber() != null) {
                    LOGGER.info("Successfully upload to s3, eTag: {}", completedPart.eTag());
                    printOperationStatus("UploadPart", true);
                } else {
                    String exceptionMsg = String.format("Failed to uploadPart to s3, eTag: %s, partNumber: %s", completedPart.eTag(), completedPart.partNumber());
                    handleErrorResponse(exceptionMsg, "UploadPart");
                }
                return completedPart;
            })
            .exceptionally(ex -> {
                handleException(ex, "UploadPart");
                return null;
            });
    }

    private CompletionStage<Void> checkCompleteMultipartUpload(String path, String uploadId, CompletedPart completedPart) {
        List<CompletedPart> parts = new ArrayList<>();
        parts.add(completedPart);
        return s3Operator.completeMultipartUpload(path, uploadId, parts).thenRun(() -> {
            LOGGER.info("Successfully CompleteMultipartUpload");
            printOperationStatus("CompleteMultipartUpload", true);
        }).exceptionally(ex -> handleException(ex, "completeMultipartUpload"));
    }

    private CompletableFuture<Void> checkUploadPartCopy() {
        String sourcePath = String.format("check_upload_part_copy_available/%d", System.nanoTime());
        String path = String.format("check_upload_part_copy_available/%d", System.nanoTime());

        byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
        ByteBuf data = Unpooled.wrappedBuffer(content);

        return s3Operator.write(sourcePath, data)
            .thenCompose(aVoid -> s3Operator.createMultipartUpload(path))
            .thenCompose(uploadId ->
                s3Operator.uploadPartCopy(sourcePath, path, 0, data.readableBytes(), uploadId, 1)
                    .thenCompose(uploadPartCopyResponse -> {
                        if (uploadPartCopyResponse != null && uploadPartCopyResponse.eTag() != null && !uploadPartCopyResponse.eTag().isEmpty()) {
                            LOGGER.info("Successfully uploadPartCopy");
                            printOperationStatus("UploadPartCopy", true);
                            CompletedPart completedPart = CompletedPart.builder().partNumber(1).eTag(uploadPartCopyResponse.eTag()).build();
                            ArrayList<CompletedPart> completedParts = new ArrayList<>();
                            completedParts.add(completedPart);
                            return s3Operator.completeMultipartUpload(path, uploadId, completedParts);
                        } else {
                            handleErrorResponse("Failed to uploadPartCopy. The response was wrong", "UploadPartCopy");
                        }
                        return null;
                    })
                    .exceptionally(ex -> handleException(ex, "UploadPartCopy")))
            .thenCompose(aVoid -> {
                CompletableFuture<Void> deletePath = s3Operator.delete(path);
                CompletableFuture<Void> deleteSourcePath = s3Operator.delete(sourcePath);
                return CompletableFuture.allOf(deletePath, deleteSourcePath);
            });
    }

    private CompletableFuture<Object> checkDeleteObjs() {
        byte[] content = new Date().toString().getBytes(StandardCharsets.UTF_8);
        String path1 = String.format("check_available/deleteObjects/%d", System.nanoTime());
        String path2 = String.format("check_available/deleteObjects/%d", System.nanoTime() + 1);
        List<String> paths = List.of(path1, path2);
        CompletableFuture<Void> writeFuture = CompletableFuture.allOf(
            s3Operator.write(path1, Unpooled.wrappedBuffer(content)),
            s3Operator.write(path2, Unpooled.wrappedBuffer(content))
        );

        return writeFuture.thenCompose(v ->
            s3Operator.delete(paths)
                .thenCompose(response -> {
                    if (response != null && response.size() == 2) {
                        LOGGER.info("Successfully deleted objects");
                        printOperationStatus("Delete objects", true);
                        return CompletableFuture.completedFuture(null);
                    } else {
                        String exceptionMsg = "Failed to delete objects. The response was wrong";
                        handleErrorResponse(exceptionMsg, "Delete objects");
                    }
                    return null;
                }).exceptionally(ex -> handleException(ex, "deleteObjects"))
        );
    }

    private void handleErrorResponse(String exceptionMsg, String operation) {
        printOperationStatus(operation, false);
        LOGGER.error(exceptionMsg);
        throw new RuntimeException(exceptionMsg);
    }

    private Void handleException(Throwable ex, String operation) {
        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
        cause = ex.getCause();
        String exceptionMsg = String.format("Failed to %s on S3. Here are your parameters about S3: %s.", operation, this);
        if (cause instanceof SdkClientException) {
            if (cause.getMessage().contains("UnknownHostException")) {
                Throwable rootCause = ExceptionUtils.getRootCause(cause);
                exceptionMsg += "\nUnable to resolve Host \"" + rootCause.getMessage() + "\". Please check your S3 endpoint.";
            } else if (cause.getMessage().startsWith("Unable to execute HTTP request")) {
                exceptionMsg += "\nUnable to execute HTTP request. Please check your network connection and make sure you can access S3.";
            }
        } else if (cause instanceof ExecutionException) {
            Throwable realCause = cause.getCause();
            if (realCause instanceof NoSuchBucketException) {
                exceptionMsg += "\nBucket \"" + bucket + "\" not found. Please check your bucket name.";
            }
        } else if (cause instanceof S3Exception) {
            if (cause instanceof NoSuchBucketException) {
                exceptionMsg += "\nBucket \"" + bucket + "\" not found. Please check your bucket name.";
            }
        }
        printOperationStatus(operation, false);
        List<String> advices = advices();
        if (!advices.isEmpty()) {
            exceptionMsg += "\nHere are some advices: \n" + String.join("\n", advices);
        }
        throw new RuntimeException(exceptionMsg, ex);
    }

    private Void handleRangeReadException(Throwable ex) {
        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
        if (cause instanceof NoSuchKeyException) {
            printOperationStatus("read object", false);
            throw new RuntimeException("Failed to rangeRead object. No such key.", ex);
        } else {
            return handleException(ex, "read");
        }
    }

    private void printOperationStatus(String operation, boolean isSuccess) {
        if (needPrintToConsole) {
            if (isSuccess) {
                System.out.println("[ OK ] " + operation);
            } else {
                System.out.println("[ FAILED ] " + operation);
            }
        }
    }

    private List<String> advices() {
        List<String> advises = new ArrayList<>();
        checkBucketName(advises);
        checkEndpoint(advises);
        checkCredentialsProvider(advises);
        checkRegion(advises);
        checkForcePathStyle(advises);
        checkTagging(advises);
        return advises;
    }

    private void checkBucketName(List<String> advises) {
        if (StringUtils.isBlank(bucket)) {
            advises.add("bucketName is blank. Please supply a valid bucketName.");
        }
    }

    private void checkEndpoint(List<String> advises) {
        if (StringUtils.isBlank(endpoint)) {
            advises.add("endpoint is blank. Please supply a valid endpoint.");
        } else {
            validateEndpoint(advises);
        }
    }

    private void validateEndpoint(List<String> advises) {
        if (endpoint.startsWith("https")) {
            advises.add("You are using https endpoint. Please make sure your object storage service supports https.");
        }
        String[] splits = endpoint.split("//");
        if (splits.length < 2) {
            advises.add("endpoint is invalid. Please supply a valid endpoint.");
        } else {
            String[] dotSplits = splits[1].split("\\.");
            if (dotSplits.length == 0 || StringUtils.isBlank(dotSplits[0])) {
                advises.add("endpoint is invalid. Please supply a valid endpoint.");
            } else if (!StringUtils.isBlank(bucket) && Objects.equals(bucket.toLowerCase(Locale.ENGLISH), dotSplits[0].toLowerCase(Locale.ENGLISH))) {
                advises.add("bucket name should not be included in endpoint.");
            }
        }
    }

    private void checkCredentialsProvider(List<String> advises) {
        if (credentialsProviders == null || credentialsProviders.isEmpty()) {
            advises.add("no credentials provider is supplied. Please supply a credentials provider.");
        } else {
            try {
                AwsCredentialsProviderChain chain = AwsCredentialsProviderChain.builder().credentialsProviders(credentialsProviders).build();
                chain.resolveCredentials();
            } catch (SdkClientException e) {
                advises.add("all provided credentials providers are invalid. Please supply a valid credentials provider. Error msg: " + e.getMessage());
            }
        }
    }

    private void checkRegion(List<String> advises) {
        if (StringUtils.isBlank(region)) {
            advises.add("region is blank. Please supply a valid region.");
        }
    }

    private void checkForcePathStyle(List<String> advises) {
        if (!isForcePathStyle) {
            advises.add("forcePathStyle is set as false. Please set it as true if you are using minio.");
        }
    }

    private void checkTagging(List<String> advises) {
        if (tagging) {
            advises.add("currently, it's only supported in AWS S3. Please make sure your object storage service supports tagging.");
        }
    }
}

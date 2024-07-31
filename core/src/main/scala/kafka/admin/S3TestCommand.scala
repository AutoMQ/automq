/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.admin

import com.automq.stream.s3.operator.BucketURI
import com.automq.stream.utils.PingS3Helper
import joptsimple.OptionSpec
import kafka.utils.{Exit, Logging}
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}

import java.util

object S3TestCommand extends Logging {
  def main(args: Array[String]): Unit = {
    try {
      val opts = new S3CommandOptions(args)

      CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to test your access to s3")

      opts.checkArgs()

      processCommand(opts)
    } catch {
      case t: Throwable =>
        logger.debug(s"Error while executing s3 test command with args '${args.mkString(" ")}'", t)
        System.err.println(s"Error while executing s3 test command with args '${args.mkString(" ")}'")
        t.printStackTrace(System.err)
        Exit.exit(1)
    }
  }

  private def processCommand(opts: S3CommandOptions): Unit = {
    val s3Endpoint = opts.options.valueOf(opts.s3EndpointOpt)
    val s3Bucket = opts.options.valueOf(opts.s3BucketOpt)
    val s3Region = opts.options.valueOf(opts.s3RegionOpt)
    val s3AccessKey = opts.options.valueOf(opts.s3AccessKeyOpt)
    val s3SecretKey = opts.options.valueOf(opts.s3SecretKeyOpt)
    val forcePathStyle = opts.has(opts.forcePathStyleOpt)
    val tagging = opts.has(opts.tagging)

    val bucketURLStr = "0@s3://" + s3Bucket + "?region=" + s3Region + "&endpoint=" + s3Endpoint + "&pathStyle=" + forcePathStyle + "&accessKey=" + s3AccessKey + "&secretKey=" + s3SecretKey

    val pingS3Helper = PingS3Helper.builder()
      .bucket(BucketURI.parse(bucketURLStr))
      .tagging(if (tagging) util.Map.of("test-tag-key", "test-tag-value") else null)
      .needPrintToConsole(true)
      .build()
    pingS3Helper.pingS3()
  }

  class S3CommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val s3EndpointOpt = parser.accepts("endpoint", "The S3 endpoint to connect to. Note that bucket name SHOULD NOT be included in the endpoint.")
      .withRequiredArg
      .describedAs("s3 endpoint to connect to")
      .ofType(classOf[String])
    val s3BucketOpt = parser.accepts("bucket", "The S3 bucket to connect to.")
      .withRequiredArg
      .describedAs("s3 bucket")
      .ofType(classOf[String])
    val s3RegionOpt = parser.accepts("region", "The S3 region to connect to.")
      .withRequiredArg
      .describedAs("s3 region")
      .ofType(classOf[String])
    val s3AccessKeyOpt = parser.accepts("ak", "The S3 access key to use.")
      .withRequiredArg
      .describedAs("s3 access key")
      .ofType(classOf[String])
    val s3SecretKeyOpt = parser.accepts("sk", "The S3 secret key to use.")
      .withRequiredArg()
      .describedAs("s3 secret key")
      .ofType(classOf[String])
    val forcePathStyleOpt = parser.accepts("force-path-style", "Force path style access. Set it if you are using minio. " +
      "As a result, the bucket name is always left in the request URI and never moved to the host as a sub-domain.")
    val tagging = parser.accepts("tagging", "Whether to test tagging access.")

    options = parser.parse(args: _*)

    def has(builder: OptionSpec[_]): Boolean = options.has(builder)

    def checkArgs(): Unit = {
      CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to test S3 access.")

      // check required args
      if (!has(s3EndpointOpt))
        throw new IllegalArgumentException("--endpoint must be specified")
      if (!has(s3BucketOpt))
        throw new IllegalArgumentException("--bucket must be specified")
      if (!has(s3RegionOpt))
        throw new IllegalArgumentException("--region must be specified")
      if (!has(s3AccessKeyOpt))
        throw new IllegalArgumentException("--ak must be specified")
      if (!has(s3SecretKeyOpt))
        throw new IllegalArgumentException("--sk must be specified")

    }
  }
}

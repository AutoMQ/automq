/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.nio.ByteBuffer

import joptsimple._
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.apache.kafka.common.utils.{Base64, SecurityUtils, Utils}

import scala.collection.JavaConverters._
import scala.collection.Set

/**
 * A command to manage delegation token.
 */
object DelegationTokenCommand extends Logging {

  def main(args: Array[String]): Unit = {
    val opts = new DelegationTokenCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Tool to create, renew, expire, or describe delegation tokens.")

    // should have exactly one action
    val actions = Seq(opts.createOpt, opts.renewOpt, opts.expiryOpt, opts.describeOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --create, --renew, --expire or --describe")

    opts.checkArgs()

    val adminClient = createAdminClient(opts)

    var exitCode = 0
    try {
      if(opts.options.has(opts.createOpt))
        createToken(adminClient, opts)
      else if(opts.options.has(opts.renewOpt))
        renewToken(adminClient, opts)
      else if(opts.options.has(opts.expiryOpt))
        expireToken(adminClient, opts)
      else if(opts.options.has(opts.describeOpt))
        describeToken(adminClient, opts)
    } catch {
      case e: Throwable =>
        println("Error while executing delegation token command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      adminClient.close()
      Exit.exit(exitCode)
    }
  }

  def createToken(adminClient: AdminClient, opts: DelegationTokenCommandOptions) = {
    val renewerPrincipals = getPrincipals(opts, opts.renewPrincipalsOpt)
    val maxLifeTimeMs = opts.options.valueOf(opts.maxLifeTimeOpt).longValue

    println("Calling create token operation with renewers :" + renewerPrincipals +" , max-life-time-period :"+ maxLifeTimeMs)
    val response = adminClient.createToken(renewerPrincipals, maxLifeTimeMs)
    response  match {
        case (Errors.NONE, token) => println("Created delegation token with tokenId : %s".format(token.tokenInfo.tokenId)); printToken(List(token))
        case (e, _) =>  throw new AdminOperationException(e.message)
    }
  }

  def printToken(tokens: List[DelegationToken]): Unit = {
    print("\n%-15s %-30s %-15s %-25s %-15s %-15s %-15s".format("TOKENID", "HMAC", "OWNER", "RENEWERS", "ISSUEDATE", "EXPIRYDATE", "MAXDATE"))
    for (token <- tokens) {
      val tokenInfo = token.tokenInfo
      print("\n%-15s %-30s %-15s %-25s %-15s %-15s %-15s".format(
        tokenInfo.tokenId,
        token.hmacAsBase64String,
        tokenInfo.owner,
        tokenInfo.renewersAsString,
        tokenInfo.issueTimestamp,
        tokenInfo.expiryTimestamp,
        tokenInfo.maxTimestamp))
      println()
    }
  }

  private def getPrincipals(opts: DelegationTokenCommandOptions, principalOptionSpec: ArgumentAcceptingOptionSpec[String]): List[KafkaPrincipal] = {
    if (opts.options.has(principalOptionSpec))
      opts.options.valuesOf(principalOptionSpec).asScala.map(s => SecurityUtils.parseKafkaPrincipal(s.trim)).toList
    else
      List.empty[KafkaPrincipal]
  }

  def renewToken(adminClient: AdminClient, opts: DelegationTokenCommandOptions) = {
    val hmac = opts.options.valueOf(opts.hmacOpt)
    val renewTimePeriodMs = opts.options.valueOf(opts.renewTimePeriodOpt).longValue()
    println("Calling renew token operation with hmac :" + hmac +" , renew-time-period :"+ renewTimePeriodMs)
    val response = adminClient.renewToken(ByteBuffer.wrap(Base64.decoder.decode(hmac)), renewTimePeriodMs)
    response match {
      case (Errors.NONE, expiryTimeStamp) => println("Completed renew operation. New expiry timestamp : %s".format(expiryTimeStamp))
      case (e, expiryTimeStamp) => throw new AdminOperationException(e.message)
    }
  }

  def expireToken(adminClient: AdminClient, opts: DelegationTokenCommandOptions) = {
    val hmac = opts.options.valueOf(opts.hmacOpt)
    val expiryTimePeriodMs = opts.options.valueOf(opts.expiryTimePeriodOpt).longValue()
    println("Calling expire token operation with hmac :" + hmac +" , expire-time-period : "+ expiryTimePeriodMs)
    val response = adminClient.expireToken(ByteBuffer.wrap(Base64.decoder.decode(hmac)), expiryTimePeriodMs)
    response match {
      case (Errors.NONE, expiryTimeStamp) => println("Completed expire operation. New expiry timestamp : %s".format(expiryTimeStamp))
      case (e, expiryTimeStamp) => throw new AdminOperationException(e.message)
    }
  }

  def describeToken(adminClient: AdminClient, opts: DelegationTokenCommandOptions) = {
    val ownerPrincipals = getPrincipals(opts, opts.ownerPrincipalsOpt)
    println("Calling describe token operation for owners :" + ownerPrincipals)
    val response = adminClient.describeToken(ownerPrincipals)
    response  match {
      case (Errors.NONE, tokens) => println("Total Number of tokens : %s".format(tokens.size)); printToken(tokens)
      case (e, tokens) => throw new AdminOperationException(e.message)
    }
  }

  private def createAdminClient(opts: DelegationTokenCommandOptions): AdminClient = {
    val props = Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
    AdminClient.create(props)
  }

  class DelegationTokenCommandOptions(args: Array[String]) {
    val BootstrapServerDoc = "REQUIRED: server(s) to use for bootstrapping."
    val CommandConfigDoc = "REQUIRED: A property file containing configs to be passed to Admin Client. Token management" +
      " operations are allowed in secure mode only. This config file is used to pass security related configs."

    val parser = new OptionParser(false)
    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
                                   .withRequiredArg
                                   .ofType(classOf[String])
    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
      .withRequiredArg
      .ofType(classOf[String])

    val createOpt = parser.accepts("create", "Create a new delegation token.")
    val renewOpt = parser.accepts("renew",  "Renew delegation token.")
    val expiryOpt = parser.accepts("expire", "Expire delegation token.")
    val describeOpt = parser.accepts("describe", "describe delegation tokens.")

    val ownerPrincipalsOpt = parser.accepts("owner-principal", "owner is a kafka principal. It is should be in principalType:name format.")
      .withOptionalArg()
      .ofType(classOf[String])

    val renewPrincipalsOpt = parser.accepts("renewer-principal", "renewer is a kafka principal. It is should be in principalType:name format.")
      .withOptionalArg()
      .ofType(classOf[String])

    val maxLifeTimeOpt = parser.accepts("max-life-time-period", "Max life period for the token in milliseconds. If the value is -1," +
      " then token max life time will default to a server side config value (delegation.token.max.lifetime.ms).")
      .withOptionalArg()
      .ofType(classOf[Long])

    val renewTimePeriodOpt = parser.accepts("renew-time-period", "Renew time period in milliseconds. If the value is -1, then the" +
      " renew time period will default to a server side config value (delegation.token.expiry.time.ms).")
      .withOptionalArg()
      .ofType(classOf[Long])

    val expiryTimePeriodOpt = parser.accepts("expiry-time-period", "Expiry time period in milliseconds. If the value is -1, then the" +
      " token will get invalidated immediately." )
      .withOptionalArg()
      .ofType(classOf[Long])

    val hmacOpt = parser.accepts("hmac", "HMAC of the delegation token")
      .withOptionalArg
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, commandConfigOpt)

      if (options.has(createOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, maxLifeTimeOpt)

      if (options.has(renewOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, hmacOpt, renewTimePeriodOpt)

      if (options.has(expiryOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, hmacOpt, expiryTimePeriodOpt)

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, createOpt, Set(hmacOpt, renewTimePeriodOpt, expiryTimePeriodOpt, ownerPrincipalsOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, renewOpt, Set(renewPrincipalsOpt, maxLifeTimeOpt, expiryTimePeriodOpt, ownerPrincipalsOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, expiryOpt, Set(renewOpt, maxLifeTimeOpt, renewTimePeriodOpt, ownerPrincipalsOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, describeOpt, Set(renewTimePeriodOpt, maxLifeTimeOpt, hmacOpt, renewTimePeriodOpt, expiryTimePeriodOpt))
    }
  }
}

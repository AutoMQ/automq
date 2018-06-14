/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import joptsimple._
import joptsimple.util.EnumConverter
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.resource.{PatternType, ResourcePatternFilter, Resource => JResource, ResourceType => JResourceType}

import scala.collection.JavaConverters._

object AclCommand extends Logging {

  val ClusterResourceFilter = new ResourcePatternFilter(JResourceType.CLUSTER, JResource.CLUSTER_NAME, PatternType.LITERAL)

  private val Newline = scala.util.Properties.lineSeparator

  val ResourceTypeToValidOperations: Map[JResourceType, Set[Operation]] = Map[JResourceType, Set[Operation]](
    JResourceType.TOPIC -> Set(Read, Write, Create, Describe, Delete, DescribeConfigs, AlterConfigs, All),
    JResourceType.GROUP -> Set(Read, Describe, Delete, All),
    JResourceType.CLUSTER -> Set(Create, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite, Alter, Describe, All),
    JResourceType.TRANSACTIONAL_ID -> Set(Describe, Write, All),
    JResourceType.DELEGATION_TOKEN -> Set(Describe, All)
  )

  def main(args: Array[String]) {

    val opts = new AclCommandOptions(args)

    if (opts.options.has(opts.helpOpt))
      CommandLineUtils.printUsageAndDie(opts.parser, "Usage:")

    opts.checkArgs()

    try {
      if (opts.options.has(opts.addOpt))
        addAcl(opts)
      else if (opts.options.has(opts.removeOpt))
        removeAcl(opts)
      else if (opts.options.has(opts.listOpt))
        listAcl(opts)
    } catch {
      case e: Throwable =>
        println(s"Error while executing ACL command: ${e.getMessage}")
        println(Utils.stackTrace(e))
        Exit.exit(1)
    }
  }

  def withAuthorizer(opts: AclCommandOptions)(f: Authorizer => Unit) {
    val defaultProps = Map(KafkaConfig.ZkEnableSecureAclsProp -> JaasUtils.isZkSecurityEnabled)
    val authorizerProperties =
      if (opts.options.has(opts.authorizerPropertiesOpt)) {
        val authorizerProperties = opts.options.valuesOf(opts.authorizerPropertiesOpt).asScala
        defaultProps ++ CommandLineUtils.parseKeyValueArgs(authorizerProperties, acceptMissingValue = false).asScala
      } else {
        defaultProps
      }

    val authorizerClass = opts.options.valueOf(opts.authorizerOpt)
    val authZ = CoreUtils.createObject[Authorizer](authorizerClass)
    try {
      authZ.configure(authorizerProperties.asJava)
      f(authZ)
    }
    finally CoreUtils.swallow(authZ.close(), this)
  }

  private def addAcl(opts: AclCommandOptions) {
    val patternType: PatternType = opts.options.valueOf(opts.resourcePatternType)
    if (patternType == PatternType.MATCH || patternType == PatternType.ANY)
      CommandLineUtils.printUsageAndDie(opts.parser, s"A '--resource-pattern-type' value of '$patternType' is not valid when adding acls.")

    withAuthorizer(opts) { authorizer =>
      val resourceToAcl = getResourceFilterToAcls(opts).map {
        case (filter, acls) =>
          Resource(ResourceType.fromJava(filter.resourceType()), filter.name(), filter.patternType()) -> acls
      }

      if (resourceToAcl.values.exists(_.isEmpty))
        CommandLineUtils.printUsageAndDie(opts.parser, "You must specify one of: --allow-principal, --deny-principal when trying to add ACLs.")

      for ((resource, acls) <- resourceToAcl) {
        println(s"Adding ACLs for resource `$resource`: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
        authorizer.addAcls(acls, resource)
      }

      listAcl(opts)
    }
  }

  private def removeAcl(opts: AclCommandOptions) {
    withAuthorizer(opts) { authorizer =>
      val filterToAcl = getResourceFilterToAcls(opts)

      for ((filter, acls) <- filterToAcl) {
        if (acls.isEmpty) {
          if (confirmAction(opts, s"Are you sure you want to delete all ACLs for resource filter `$filter`? (y/n)"))
            removeAcls(authorizer, acls, filter)
        } else {
          if (confirmAction(opts, s"Are you sure you want to remove ACLs: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline from resource filter `$filter`? (y/n)"))
            removeAcls(authorizer, acls, filter)
        }
      }

      listAcl(opts)
    }
  }

  private def removeAcls(authorizer: Authorizer, acls: Set[Acl], filter: ResourcePatternFilter) {
    getAcls(authorizer, filter)
      .keys
      .foreach(resource =>
        if (acls.isEmpty) authorizer.removeAcls(resource)
        else authorizer.removeAcls(acls, resource)
      )
  }

  private def listAcl(opts: AclCommandOptions) {
    withAuthorizer(opts) { authorizer =>
      val filters = getResourceFilter(opts, dieIfNoResourceFound = false)

      val resourceToAcls: Iterable[(Resource, Set[Acl])] =
        if (filters.isEmpty) authorizer.getAcls()
        else filters.flatMap(filter => getAcls(authorizer, filter))

      for ((resource, acls) <- resourceToAcls)
        println(s"Current ACLs for resource `$resource`: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
    }
  }

  private def getAcls(authorizer: Authorizer, filter: ResourcePatternFilter): Map[Resource, Set[Acl]] =
    authorizer.getAcls()
      .filter { case (resource, acl) => filter.matches(resource.toPattern) }

  private def getResourceFilterToAcls(opts: AclCommandOptions): Map[ResourcePatternFilter, Set[Acl]] = {
    var resourceToAcls = Map.empty[ResourcePatternFilter, Set[Acl]]

    //if none of the --producer or --consumer options are specified , just construct ACLs from CLI options.
    if (!opts.options.has(opts.producerOpt) && !opts.options.has(opts.consumerOpt)) {
      resourceToAcls ++= getCliResourceFilterToAcls(opts)
    }

    //users are allowed to specify both --producer and --consumer options in a single command.
    if (opts.options.has(opts.producerOpt))
      resourceToAcls ++= getProducerResourceFilterToAcls(opts)

    if (opts.options.has(opts.consumerOpt))
      resourceToAcls ++= getConsumerResourceFilterToAcls(opts).map { case (k, v) => k -> (v ++ resourceToAcls.getOrElse(k, Set.empty[Acl])) }

    validateOperation(opts, resourceToAcls)

    resourceToAcls
  }

  private def getProducerResourceFilterToAcls(opts: AclCommandOptions): Map[ResourcePatternFilter, Set[Acl]] = {
    val filters = getResourceFilter(opts)

    val topics: Set[ResourcePatternFilter] = filters.filter(_.resourceType == JResourceType.TOPIC)
    val transactionalIds: Set[ResourcePatternFilter] = filters.filter(_.resourceType == JResourceType.TRANSACTIONAL_ID)
    val enableIdempotence = opts.options.has(opts.idempotentOpt)

    val topicAcls = getAcl(opts, Set(Write, Describe, Create))
    val transactionalIdAcls = getAcl(opts, Set(Write, Describe))

    //Write, Describe, Create permission on topics, Write, Describe on transactionalIds
    topics.map(_ -> topicAcls).toMap ++
      transactionalIds.map(_ -> transactionalIdAcls).toMap ++
        (if (enableIdempotence)
          Map(ClusterResourceFilter -> getAcl(opts, Set(IdempotentWrite)))
        else
          Map.empty)
  }

  private def getConsumerResourceFilterToAcls(opts: AclCommandOptions): Map[ResourcePatternFilter, Set[Acl]] = {
    val filters = getResourceFilter(opts)

    val topics: Set[ResourcePatternFilter] = filters.filter(_.resourceType == JResourceType.TOPIC)
    val groups: Set[ResourcePatternFilter] = filters.filter(_.resourceType == JResourceType.GROUP)

    //Read, Describe on topic, Read on consumerGroup

    val acls = getAcl(opts, Set(Read, Describe))

    topics.map(_ -> acls).toMap[ResourcePatternFilter, Set[Acl]] ++
      groups.map(_ -> getAcl(opts, Set(Read))).toMap[ResourcePatternFilter, Set[Acl]]
  }

  private def getCliResourceFilterToAcls(opts: AclCommandOptions): Map[ResourcePatternFilter, Set[Acl]] = {
    val acls = getAcl(opts)
    val filters = getResourceFilter(opts)
    filters.map(_ -> acls).toMap
  }

  private def getAcl(opts: AclCommandOptions, operations: Set[Operation]): Set[Acl] = {
    val allowedPrincipals = getPrincipals(opts, opts.allowPrincipalsOpt)

    val deniedPrincipals = getPrincipals(opts, opts.denyPrincipalsOpt)

    val allowedHosts = getHosts(opts, opts.allowHostsOpt, opts.allowPrincipalsOpt)

    val deniedHosts = getHosts(opts, opts.denyHostsOpt, opts.denyPrincipalsOpt)

    val acls = new collection.mutable.HashSet[Acl]
    if (allowedHosts.nonEmpty && allowedPrincipals.nonEmpty)
      acls ++= getAcls(allowedPrincipals, Allow, operations, allowedHosts)

    if (deniedHosts.nonEmpty && deniedPrincipals.nonEmpty)
      acls ++= getAcls(deniedPrincipals, Deny, operations, deniedHosts)

    acls.toSet
  }

  private def getAcl(opts: AclCommandOptions): Set[Acl] = {
    val operations = opts.options.valuesOf(opts.operationsOpt).asScala.map(operation => Operation.fromString(operation.trim)).toSet
    getAcl(opts, operations)
  }

  def getAcls(principals: Set[KafkaPrincipal], permissionType: PermissionType, operations: Set[Operation],
              hosts: Set[String]): Set[Acl] = {
    for {
      principal <- principals
      operation <- operations
      host <- hosts
    } yield new Acl(principal, permissionType, host, operation)
  }

  private def getHosts(opts: AclCommandOptions, hostOptionSpec: ArgumentAcceptingOptionSpec[String],
                       principalOptionSpec: ArgumentAcceptingOptionSpec[String]): Set[String] = {
    if (opts.options.has(hostOptionSpec))
      opts.options.valuesOf(hostOptionSpec).asScala.map(_.trim).toSet
    else if (opts.options.has(principalOptionSpec))
      Set[String](Acl.WildCardHost)
    else
      Set.empty[String]
  }

  private def getPrincipals(opts: AclCommandOptions, principalOptionSpec: ArgumentAcceptingOptionSpec[String]): Set[KafkaPrincipal] = {
    if (opts.options.has(principalOptionSpec))
      opts.options.valuesOf(principalOptionSpec).asScala.map(s => KafkaPrincipal.fromString(s.trim)).toSet
    else
      Set.empty[KafkaPrincipal]
  }

  private def getResourceFilter(opts: AclCommandOptions, dieIfNoResourceFound: Boolean = true): Set[ResourcePatternFilter] = {
    val patternType: PatternType = opts.options.valueOf(opts.resourcePatternType)

    var resourceFilters = Set.empty[ResourcePatternFilter]
    if (opts.options.has(opts.topicOpt))
      opts.options.valuesOf(opts.topicOpt).asScala.foreach(topic => resourceFilters += new ResourcePatternFilter(JResourceType.TOPIC, topic.trim, patternType))

    if (patternType == PatternType.LITERAL && (opts.options.has(opts.clusterOpt) || opts.options.has(opts.idempotentOpt)))
      resourceFilters += ClusterResourceFilter

    if (opts.options.has(opts.groupOpt))
      opts.options.valuesOf(opts.groupOpt).asScala.foreach(group => resourceFilters += new ResourcePatternFilter(JResourceType.GROUP, group.trim, patternType))

    if (opts.options.has(opts.transactionalIdOpt))
      opts.options.valuesOf(opts.transactionalIdOpt).asScala.foreach(transactionalId =>
        resourceFilters += new ResourcePatternFilter(JResourceType.TRANSACTIONAL_ID, transactionalId, patternType))

    if (opts.options.has(opts.delegationTokenOpt))
      opts.options.valuesOf(opts.delegationTokenOpt).asScala.foreach(token => resourceFilters += new ResourcePatternFilter(JResourceType.DELEGATION_TOKEN, token.trim, patternType))

    if (resourceFilters.isEmpty && dieIfNoResourceFound)
      CommandLineUtils.printUsageAndDie(opts.parser, "You must provide at least one resource: --topic <topic> or --cluster or --group <group> or --delegation-token <Delegation Token ID>")

    resourceFilters
  }

  private def confirmAction(opts: AclCommandOptions, msg: String): Boolean = {
    if (opts.options.has(opts.forceOpt))
        return true
    println(msg)
    Console.readLine().equalsIgnoreCase("y")
  }

  private def validateOperation(opts: AclCommandOptions, resourceToAcls: Map[ResourcePatternFilter, Set[Acl]]): Unit = {
    for ((resource, acls) <- resourceToAcls) {
      val validOps = ResourceTypeToValidOperations(resource.resourceType)
      if ((acls.map(_.operation) -- validOps).nonEmpty)
        CommandLineUtils.printUsageAndDie(opts.parser, s"ResourceType ${resource.resourceType} only supports operations ${validOps.mkString(",")}")
    }
  }

  class AclCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val authorizerOpt = parser.accepts("authorizer", "Fully qualified class name of the authorizer, defaults to kafka.security.auth.SimpleAclAuthorizer.")
      .withRequiredArg
      .describedAs("authorizer")
      .ofType(classOf[String])
      .defaultsTo(classOf[SimpleAclAuthorizer].getName)

    val authorizerPropertiesOpt = parser.accepts("authorizer-properties", "REQUIRED: properties required to configure an instance of Authorizer. " +
      "These are key=val pairs. For the default authorizer the example values are: zookeeper.connect=localhost:2181")
      .withRequiredArg
      .describedAs("authorizer-properties")
      .ofType(classOf[String])

    val topicOpt = parser.accepts("topic", "topic to which ACLs should be added or removed. " +
      "A value of * indicates ACL should apply to all topics.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])

    val clusterOpt = parser.accepts("cluster", "Add/Remove cluster ACLs.")
    val groupOpt = parser.accepts("group", "Consumer Group to which the ACLs should be added or removed. " +
      "A value of * indicates the ACLs should apply to all groups.")
      .withRequiredArg
      .describedAs("group")
      .ofType(classOf[String])

    val transactionalIdOpt = parser.accepts("transactional-id", "The transactionalId to which ACLs should " +
      "be added or removed. A value of * indicates the ACLs should apply to all transactionalIds.")
      .withRequiredArg
      .describedAs("transactional-id")
      .ofType(classOf[String])

    val idempotentOpt = parser.accepts("idempotent", "Enable idempotence for the producer. This should be " +
      "used in combination with the --producer option. Note that idempotence is enabled automatically if " +
      "the producer is authorized to a particular transactional-id.")

    val delegationTokenOpt = parser.accepts("delegation-token", "Delegation token to which ACLs should be added or removed. " +
      "A value of * indicates ACL should apply to all tokens.")
      .withRequiredArg
      .describedAs("delegation-token")
      .ofType(classOf[String])

    val resourcePatternType = parser.accepts("resource-pattern-type", "The type of the resource pattern or pattern filter. " +
      "When adding acls, this should be a specific pattern type, e.g. 'literal' or 'prefixed'. " +
      "When listing or removing acls, a specific pattern type can be used to list or remove acls from specific resource patterns, " +
      "or use the filter values of 'any' or 'match', where 'any' will match any pattern type, but will match the resource name exactly, " +
      "where as 'match' will perform pattern matching to list or remove all acls that affect the supplied resource(s). " +
      "WARNING: 'match', when used in combination with the '--remove' switch, should be used with care.")
      .withRequiredArg()
      .ofType(classOf[String])
      .withValuesConvertedBy(new PatternTypeConverter())
      .defaultsTo(PatternType.LITERAL)

    val addOpt = parser.accepts("add", "Indicates you are trying to add ACLs.")
    val removeOpt = parser.accepts("remove", "Indicates you are trying to remove ACLs.")
    val listOpt = parser.accepts("list", "List ACLs for the specified resource, use --topic <topic> or --group <group> or --cluster to specify a resource.")

    val operationsOpt = parser.accepts("operation", "Operation that is being allowed or denied. Valid operation names are: " + Newline +
      Operation.values.map("\t" + _).mkString(Newline) + Newline)
      .withRequiredArg
      .ofType(classOf[String])
      .defaultsTo(All.name)

    val allowPrincipalsOpt = parser.accepts("allow-principal", "principal is in principalType:name format." +
      " Note that principalType must be supported by the Authorizer being used." +
      " For example, User:* is the wild card indicating all users.")
      .withRequiredArg
      .describedAs("allow-principal")
      .ofType(classOf[String])

    val denyPrincipalsOpt = parser.accepts("deny-principal", "principal is in principalType:name format. " +
      "By default anyone not added through --allow-principal is denied access. " +
      "You only need to use this option as negation to already allowed set. " +
      "Note that principalType must be supported by the Authorizer being used. " +
      "For example if you wanted to allow access to all users in the system but not test-user you can define an ACL that " +
      "allows access to User:* and specify --deny-principal=User:test@EXAMPLE.COM. " +
      "AND PLEASE REMEMBER DENY RULES TAKES PRECEDENCE OVER ALLOW RULES.")
      .withRequiredArg
      .describedAs("deny-principal")
      .ofType(classOf[String])

    val allowHostsOpt = parser.accepts("allow-host", "Host from which principals listed in --allow-principal will have access. " +
      "If you have specified --allow-principal then the default for this option will be set to * which allows access from all hosts.")
      .withRequiredArg
      .describedAs("allow-host")
      .ofType(classOf[String])

    val denyHostsOpt = parser.accepts("deny-host", "Host from which principals listed in --deny-principal will be denied access. " +
      "If you have specified --deny-principal then the default for this option will be set to * which denies access from all hosts.")
      .withRequiredArg
      .describedAs("deny-host")
      .ofType(classOf[String])

    val producerOpt = parser.accepts("producer", "Convenience option to add/remove ACLs for producer role. " +
      "This will generate ACLs that allows WRITE,DESCRIBE and CREATE on topic.")

    val consumerOpt = parser.accepts("consumer", "Convenience option to add/remove ACLs for consumer role. " +
      "This will generate ACLs that allows READ,DESCRIBE on topic and READ on group.")

    val helpOpt = parser.accepts("help", "Print usage information.")

    val forceOpt = parser.accepts("force", "Assume Yes to all queries and do not prompt.")

    val options = parser.parse(args: _*)

    def checkArgs() {
      CommandLineUtils.checkRequiredArgs(parser, options, authorizerPropertiesOpt)

      val actions = Seq(addOpt, removeOpt, listOpt).count(options.has)
      if (actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --list, --add, --remove. ")

      CommandLineUtils.checkInvalidArgs(parser, options, listOpt, Set(producerOpt, consumerOpt, allowHostsOpt, allowPrincipalsOpt, denyHostsOpt, denyPrincipalsOpt))

      //when --producer or --consumer is specified , user should not specify operations as they are inferred and we also disallow --deny-principals and --deny-hosts.
      CommandLineUtils.checkInvalidArgs(parser, options, producerOpt, Set(operationsOpt, denyPrincipalsOpt, denyHostsOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, consumerOpt, Set(operationsOpt, denyPrincipalsOpt, denyHostsOpt))

      if (options.has(producerOpt) && !options.has(topicOpt))
        CommandLineUtils.printUsageAndDie(parser, "With --producer you must specify a --topic")

      if (options.has(idempotentOpt) && !options.has(producerOpt))
        CommandLineUtils.printUsageAndDie(parser, "The --idempotent option is only available if --producer is set")

      if (options.has(consumerOpt) && (!options.has(topicOpt) || !options.has(groupOpt) || (!options.has(producerOpt) && (options.has(clusterOpt) || options.has(transactionalIdOpt)))))
        CommandLineUtils.printUsageAndDie(parser, "With --consumer you must specify a --topic and a --group and no --cluster or --transactional-id option should be specified.")
    }
  }

}

class PatternTypeConverter extends EnumConverter[PatternType](classOf[PatternType]) {

  override def convert(value: String): PatternType = {
    val patternType = super.convert(value)
    if (patternType.isUnknown)
      throw new ValueConversionException("Unknown resource-pattern-type: " + value)

    patternType
  }

  override def valuePattern: String = PatternType.values
    .filter(_ != PatternType.UNKNOWN)
    .mkString("|")
}

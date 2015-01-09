package spark.jobserver.io

import com.typesafe.config.{ConfigException, Config}
import org.slf4j.LoggerFactory
import scala.slick.driver.{H2Driver, MySQLDriver, JdbcProfile}
import scala.util.Properties.{envOrElse => getSysEnvOrElse}

/**
 * HDFS path information. An absolute path is required and the namenode url and
 * and port are optional.
 *
 * @param namenode HDFS namenode url (NOTE: doesn't not consider 2 namenodes, etc)
 * @param port HDFS namenode port the process is running on
 * @param path HDFS absolute path (without file) to store jars
 */
case class HdfsPathInfo(namenode: Option[String], port: Option[Int], path: String) {
  // Correctly returns the full URI to the HDFS path (local FS vs HDFS based the namenode and port)
  def uri = namenode.map("hdfs://" + _ + port.map(":" + _).getOrElse("")).getOrElse("file:///") + path
}

/**
 * JDBC configuration
 *
 * @constructor create a new JDBC configuration
 * @param url JDBC connection url
 * @param driver JDBC driver string
 * @param user JDBC connection user
 * @param password JDBC connection password
 * @param jdbcProfile JdbcProfile for this configuration
 * @param rootDir root directory for JobSqlDAO
 */
case class JdbcConfig(url: String,
                      driver: String,
                      user: String,
                      password: String,
                      jdbcProfile: JdbcProfile,
                      rootDir: String,
                      hdfsPathInfo: HdfsPathInfo)

/**
 * Trait for a JDBC configuration parser
 */
trait JdbcConfigParser {
  // Default SQL DAO configuration path
  private val sqlDaoConfigPath = "spark.jobserver.sqldao"
  // Default SQL DAO root directory
  protected final val defaultRootDir = "/tmp/spark-jobserver/sqldao/data"
  // Default JDBC connection user
  protected final val defaultUser = getSysEnvOrElse("SPARK_JOBSERVER_SQLDAO_JDBC_USER", "root")
  // Default JDBC connection password
  protected final val defaultPassword =
    getSysEnvOrElse("SPARK_JOBSERVER_SQLDAO_JDBC_PASSWORD", "password")
  // Default HDFS absolute path
  protected final val defaultHdfsPath = "/tmp/spark-jobserver/jars"
  // Default HdfsPathInfo if no HDFS config information provided
  protected final val defaultHdfsPathInfo = HdfsPathInfo(None, None, defaultHdfsPath)
  // JDBC driver string
  protected val jdbcDriver: String
  // JDBC driver profile
  protected val jdbcProfile: JdbcProfile

  /**
   * Parses a JDBC configuration
   *
   * @param config a configuration to parse
   * @return an optional JDBC configuration parsed from the given configuration
   */
  def parse(config: Config): Option[JdbcConfig]

  /**
   * Parses a JDBC configuration under a given sub path
   *
   * @param config a configuration to parse
   * @param configSubPath the sub-path to parse from
   * @return an optional JDBC configuration parsed under the given sub-path
   */
  protected final def parse(config: Config, configSubPath: String): Option[JdbcConfig] = {
    val rootDir = getOrElse(config.getString(sqlDaoConfigPath + ".rootdir"), defaultRootDir)
    val configRoot = sqlDaoConfigPath + "." + configSubPath
    if (config.hasPath(configRoot)) {
      val url = getOrElse(config.getString(configRoot + ".url"), "")
      val user = getOrElse(config.getString(configRoot + ".user"), defaultUser)
      val password = getOrElse(config.getString(configRoot + ".password"), defaultPassword)
      val hdfsPathInfo = parseHdfsPathInfo(config)
      val jdbcConfig =
        JdbcConfig(url, jdbcDriver, user, password, jdbcProfile, rootDir, hdfsPathInfo)

      // TODO: Add some validation for the HDFS URI
      if (validate(jdbcConfig)) Some(jdbcConfig) else None
    } else {
      None
    }
  }

  /**
   * Parses a HDFS path information
   * @param config a configuration to parse
   * @return an HDFS path as a HdfsPathInfo. If nothing is parse a default path is provided.
   */
  private def parseHdfsPathInfo(config: Config): HdfsPathInfo = {
    val configRoot = sqlDaoConfigPath + "." + "hdfs"
    if (config.hasPath(configRoot)) {
      val namenode = getOrElse(Some(config.getString(configRoot + ".namenode")), None)
      val port = getOrElse(Some(config.getInt(configRoot + ".port")), None)
      val path = getOrElse(config.getString(configRoot + ".path"), defaultHdfsPath)
      HdfsPathInfo(namenode, port, path)
    }
    else {
      defaultHdfsPathInfo
    }
  }

  /**
   * Checks if a JDBC configuration is valid
   *
   * @param jdbcConfig a JDBC configuration
   * @return true if the JDBC configuration is valid, and false otherwise
   */
  protected def validate(jdbcConfig: JdbcConfig): Boolean

  protected def getOrElse[T](getter: => T, default: T): T = {
    try getter catch {
      case e: ConfigException.Missing => default
    }
  }
}

/**
 * MariaDB JDBC configuration parser
 */
object MariaDbConfigParser extends JdbcConfigParser {
  private val logger = LoggerFactory.getLogger(getClass)
  // MariaDB JDBC driver string
  override protected val jdbcDriver = "org.mariadb.jdbc.Driver"
  // MariaDB JDBC driver profile
  override protected val jdbcProfile = MySQLDriver

  override def parse(config: Config): Option[JdbcConfig] = {
    parse(config, "mariadb")
  }

  override protected def validate(jdbcConfig: JdbcConfig): Boolean = {
    // Checks if the JDBC connection url has the MariaDB prefix
    val prefixMatch = jdbcConfig.url.startsWith("jdbc:mariadb:")
    if (!prefixMatch) {
      logger.info("Error: invalid MariaDB jdbc url")
    }

    prefixMatch
  }
}

/**
 * MySQL JDBC configuration parser
 */
object MySqlConfigParser extends JdbcConfigParser {
  private val logger = LoggerFactory.getLogger(getClass)
  // MySQL JDBC driver string
  override protected val jdbcDriver = "com.mysql.jdbc.Driver"
  // MySQL JDBC driver profile
  override protected val jdbcProfile = MySQLDriver

  override def parse(config: Config): Option[JdbcConfig] = {
    parse(config, "mysql")
  }

  override protected def validate(jdbcConfig: JdbcConfig): Boolean = {
    // Checks if the JDBC connection url has the MySQL prefix
    val prefixMatch = jdbcConfig.url.startsWith("jdbc:mysql:")
    if (!prefixMatch) {
      logger.info("Error: invalid mysql jdbc url")
    }

    prefixMatch
  }
}

/**
 * H2 JDBC configuration parser
 */
object H2ConfigParser extends JdbcConfigParser {
  private val logger = LoggerFactory.getLogger(getClass)
  // H2 JDBC connection url prefix
  private val jdbcUrlPrefix = "jdbc:h2:"
  // H2 JDBC driver string
  override protected val jdbcDriver = "org.h2.Driver"
  // H2 JDBC driver profile
  override protected val jdbcProfile = H2Driver

  override def parse(config: Config): Option[JdbcConfig] = {
    parse(config, "h2")
  }

  override protected def validate(jdbcConfig: JdbcConfig): Boolean = {
    // Checks if the JDBC connection url has the H2 prefix
    val prefixMatch = jdbcConfig.url.startsWith(jdbcUrlPrefix)
    if (!prefixMatch) {
      logger.info("Error: invalid h2 jdbc url")
    }

    prefixMatch
  }

  /** Returns a default H2 JDBC configuration **/
  def defaultConfig: JdbcConfig = {
    val url = jdbcUrlPrefix + "file:" + defaultRootDir + "/h2-db"

    JdbcConfig(url, jdbcDriver, "", "", jdbcProfile, defaultRootDir, defaultHdfsPathInfo)
  }
}

/**
 * A JDBC configuration parser factory
 */
object JdbcConfigParserFactory {
  /**
   * Parses a JDBC configuration
   *
   * @param config a configuration to parse
   * @return a MariaDB JDBC configuration if provided a valid configuration for MariaDB;
   *         otherwise, a MySQL JDBC configuration if provided a valid configuration for MySQL;
   *         otherwise, an H2 JDBC configuration if the given config has an valid configuration
   *         for H2;
   *         otherwise, None.
   */
  def parse(config: Config): Option[JdbcConfig] = {
    val jdbcConfig = MariaDbConfigParser.parse(config) orElse
      MySqlConfigParser.parse(config) orElse H2ConfigParser.parse(config)

    jdbcConfig
  }
}
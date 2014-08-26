package spark.jobserver.io

import com.typesafe.config.{ConfigException, Config}
import org.slf4j.LoggerFactory
import scala.slick.driver.{H2Driver, MySQLDriver, JdbcProfile}
import scala.util.Properties.{envOrElse => getSysEnvOrElse}


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
                      rootDir: String)

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
      val jdbcConfig = JdbcConfig(url, jdbcDriver, user, password, jdbcProfile, rootDir)

      if (validate(jdbcConfig)) Some(jdbcConfig) else None
    } else {
      None
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

    JdbcConfig(url, jdbcDriver, "", "", jdbcProfile, defaultRootDir)
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
   * @return a MySQL JDBC configuration if the given config has an valid configuration for MySQL;
   *         otherwise, an H2 JDBC configuration if the given config has an valid configuration
   *         for H2;
   *         otherwise, None.
   */
  def parse(config: Config): Option[JdbcConfig] = {
    val jdbcConfig = MySqlConfigParser.parse(config) orElse H2ConfigParser.parse(config)

    jdbcConfig
  }
}
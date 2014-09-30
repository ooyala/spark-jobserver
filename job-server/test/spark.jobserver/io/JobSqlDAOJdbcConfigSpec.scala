package spark.jobserver.io

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import spark.jobserver.TestJarFinder


class JobSqlDAOJdbcConfigSpec extends TestJarFinder with FunSpec with ShouldMatchers {

  describe("parse MySQL config") {
    val configStr =
      """
      spark.jobserver.sqldao {
        rootdir = /tmp/spark-job-server-test/sqldao/data

        mysql {
          url = "jdbc:mysql://localhost:3306/test"
          user = test
          password = test
        }
      }
      """

    it("should parse a valid MySQL config") {
      val config = ConfigFactory.parseString(configStr)
      val mysqlConfig = MySqlConfigParser.parse(config)

      mysqlConfig.isDefined should equal(true)
      mysqlConfig.get.user should equal("test")
      mysqlConfig.get.password should equal("test")
    }

    it("should fail to parse a MySQL config") {
      // An invalid MySQL config that has an invalid JDBC connection url prefix
      val invalidConfigStr = configStr.replace("jdbc:mysql:", "jdbc:sql:")
      val config = ConfigFactory.parseString(invalidConfigStr)
      val mysqlConfig = MySqlConfigParser.parse(config)

      mysqlConfig.isDefined should equal(false)
    }
  }

  describe("parse H2 config") {
    val configStr =
      """
      spark.jobserver.sqldao {
        rootdir = /tmp/spark-job-server-test/sqldao/data

        h2 {
          url = "jdbc:h2:file:/tmp/test"
          user = test
          password = test
        }
      }
      """

    it("should parse a valid H2 config") {
      val config = ConfigFactory.parseString(configStr)
      val h2Config = H2ConfigParser.parse(config)

      h2Config.isDefined should equal(true)
      h2Config.get.user should equal("test")
      h2Config.get.password should equal("test")
    }

    it("should fail to parse H2 config") {
      // An invalid H2 config that has an invalid JDBC connection url prefix
      val invalidConfigStr = configStr.replace("jdbc:h2:", "jdbc:hh2:")
      val config = ConfigFactory.parseString(invalidConfigStr)
      val h2Config = H2ConfigParser.parse(config)

      h2Config.isDefined should equal(false)
    }
  }

  describe("parse default config") {
    it("should return a default H2 config") {
      val config = ConfigFactory.parseString("")
      val jdbcConfigOpt = JdbcConfigParserFactory.parse(config)
      val jdbcConfig = jdbcConfigOpt.getOrElse(H2ConfigParser.defaultConfig)

      jdbcConfigOpt should equal(None)
      jdbcConfig.url.startsWith("jdbc:h2:") should equal(true)
    }
  }
}
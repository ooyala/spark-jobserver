package spark.jobserver.util

import com.typesafe.config.Config
import org.apache.spark.SparkContext

/**
 * I know how to make SparkContexts!
 * My implementing classes can be dynamically loaded using classloaders to ensure that the entire
 * SparkContext has access to certain dynamically loaded classes, for example, job jars.
 */
trait SparkContextFactory {
  /**
   * Creates a SparkContext.
   * @param config the overall system / job server Typesafe Config
   * @param contextConfig the config specific to this particular job
   * @param contextName the name of the context to start
   */
  def makeContext(config: Config, contextConfig: Config, contextName: String): SparkContext
}

/**
 * The default factory creates a standard SparkContext.
 * In the future if we want to add additional methods, etc. then we can have additional factories.
 * For example a specialized SparkContext to manage RDDs in a user-defined way.
 *
 * If you create your own SparkContextFactory, please make sure it has zero constructor args.
 */
class DefaultSparkContextFactory extends SparkContextFactory {
  import SparkJobUtils._

  def makeContext(config: Config, contextConfig: Config, contextName: String): SparkContext = {
    val conf = configToSparkConf(config, contextConfig, contextName)

    new SparkContext(conf)
  }
}

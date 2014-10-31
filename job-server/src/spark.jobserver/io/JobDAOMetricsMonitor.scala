package spark.jobserver.io

import com.codahale.metrics.Meter
import com.typesafe.config.{Config, ConfigRenderOptions}
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import ooyala.common.akka.metrics.MetricsLevel
import ooyala.common.akka.metrics.MetricsWrapper.{newHistogram, newMeter}

/**
 * This invocation handler monitors the job DAO and updates the metrics.
 *
 * @param jobDao the actual job DAO to be monitored
 * @param metricsLevel the level of details of metrics to monitor
 */
class JobDAOMetricsMonitor(jobDao: JobDAO, metricsLevel: MetricsLevel) extends InvocationHandler {
  // Basic metrics
  private val basicMetrics: Map[String, Meter] = {
    val metrics = collection.mutable.Map[String, Meter]()

    classOf[JobDAO].getMethods.foreach {
      method => {
        metrics(method.getName) = newMeter(getClass, method.getName)
      }
    }

    metrics.toMap
  }
  // Fine metrics
  private lazy val metricJarInfoSize = newHistogram(getClass, "JarSize")
  private lazy val metricJobInfoSize = newHistogram(getClass, "JobInfoSize")
  private lazy val metricJobConfigSize = newHistogram(getClass, "JobConfigSize")
  private lazy val metricContextConfigSize = newHistogram(getClass, "ContextConfigSize")
  // Handlers for updating fine metrics
  private val fineMetricsHandlers = Map[String, Array[AnyRef] => Unit](
    "saveJar" -> (
      (args: Array[AnyRef]) => {
        // Updates metric for jar size
        val appName = args(0).asInstanceOf[String]
        val jarBytes = args(2).asInstanceOf[Array[Byte]]
        metricJarInfoSize.update(appName.length + jarBytes.size)
      }),

    "saveJobConfig" -> (
      (args: Array[AnyRef]) => {
        // Updates metric for job config size
        val jobConfig = args(1).asInstanceOf[Config]
        val configStr = jobConfig.root().render(ConfigRenderOptions.concise())
        metricJobConfigSize.update(configStr.length)
      }),

    "saveJobInfo" -> (
      (args: Array[AnyRef]) => {
        // Updates metric for job info size
        val jobInfo = args(0).asInstanceOf[JobInfo]
        val jobInfoSize = jobInfo.contextName.length + jobInfo.classPath.length +
          jobInfo.error.map(_.getMessage.length).getOrElse(0)
        metricJobInfoSize.update(jobInfoSize)
      }),

    "saveContextConfig" -> (
      (args: Array[AnyRef]) => {
        // Updates metric for context config size
        val name = args(0).asInstanceOf[String]
        val config = args(1).asInstanceOf[Config]
        val configStr = config.root().render(ConfigRenderOptions.concise())
        metricContextConfigSize.update(name.length + configStr.length)
      }))

  override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
    val methodName = method.getName

    // Monitors basic metrics
    basicMetrics.get(methodName).foreach {
      _.mark
    }

    // Monitors more detailed metrics
    if (metricsLevel >= MetricsLevel.FINE) {
      fineMetricsHandlers.get(methodName).foreach {
        _(args)
      }
    }

    method.invoke(jobDao, args: _*)
  }
}

object JobDAOMetricsMonitor {

  def newInstance(jobDao: JobDAO, metricsLevel: MetricsLevel): JobDAO = {
    val metricsMonitor = new JobDAOMetricsMonitor(jobDao, metricsLevel)

    return Proxy.newProxyInstance(getClass.getClassLoader, Array(classOf[JobDAO]), metricsMonitor)
      .asInstanceOf[JobDAO]
  }
}
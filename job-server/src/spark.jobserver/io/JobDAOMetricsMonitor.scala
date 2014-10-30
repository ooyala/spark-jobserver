package spark.jobserver.io

import com.codahale.metrics.Meter
import com.typesafe.config.{Config, ConfigRenderOptions}
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.ConcurrentHashMap
import ooyala.common.akka.metrics.MetricsLevel
import ooyala.common.akka.metrics.MetricsWrapper.{newHistogram, newMeter}

/**
 * This invocation handler monitors the job DAO and updates the metrics.
 *
 * @param jobDao the actual job DAO to be monitored
 * @param metricsLevel the level of details of metrics to monitor
 */
class JobDAOMetricsMonitor(jobDao: JobDAO, metricsLevel: MetricsLevel) extends InvocationHandler {
  // Metrics
  private val basicMetrics = new ConcurrentHashMap[String, Meter]
  private lazy val metricJarInfoSize = newHistogram(getClass, "JarSize")
  private lazy val metricJobInfoSize = newHistogram(getClass, "JobInfoSize")
  private lazy val metricJobConfigSize = newHistogram(getClass, "JobConfigSize")
  private lazy val metricContextConfigSize = newHistogram(getClass, "ContextConfigSize")

  override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
    val methodName = method.getName
    var meter = basicMetrics.get(methodName)
    if (meter == null) {
      meter = newMeter(getClass, methodName)
      val old = basicMetrics.putIfAbsent(methodName, meter)
      if (old != null) {
        meter = old
      }
    }
    meter.mark

    // Monitors more detailed metrics
    if (metricsLevel >= MetricsLevel.FINE) {
      methodName match {
        case "saveJar" => {
          // Updates metric for jar size
          val jarBytes = args(2).asInstanceOf[Array[Byte]]
          metricJarInfoSize.update(jarBytes.size)
        }
        case "saveJobConfig" => {
          // Updates metric for job config size
          val jobConfig = args(1).asInstanceOf[Config]
          val configStr = jobConfig.root().render(ConfigRenderOptions.concise())
          metricJobConfigSize.update(configStr.length)
        }
        case "saveJobInfo" => {
          // Updates metric for job info size
          val jobInfo = args(0).asInstanceOf[JobInfo]
          val jobInfoSize = jobInfo.contextName.length + jobInfo.classPath.length +
            jobInfo.error.map(_.getMessage.length).getOrElse(0)
          metricJobInfoSize.update(jobInfoSize)
        }
        case "saveContextConfig" => {
          // Updates metric for context config size
          val name = args(0).asInstanceOf[String]
          val config = args(1).asInstanceOf[Config]
          val configStr = config.root().render(ConfigRenderOptions.concise())
          metricContextConfigSize.update(name.length + configStr.length)
        }
        case _ =>
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
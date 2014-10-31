package spark.jobserver.io

import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import ooyala.common.akka.metrics.{MetricsLevel, MetricsWrapper}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import spark.jobserver.InMemoryDAO

class JobDAOMetricsMonitorSpec extends FunSpec with ShouldMatchers with BeforeAndAfter {
  private val jobId = "jobId"
  private val contextName = "contextName"
  private val appName = "appName"
  private val jarBytes = Array.fill[Byte](10)(1)
  private val jarInfo = JarInfo(appName, DateTime.now)
  private val classPath = "classPath"
  private val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, DateTime.now, None, None)
  private val jobConfig = ConfigFactory.parseString("{job-name=test}")
  private val contextConfig = ConfigFactory.parseString({"num-cpu=2"})
  private val dao = new InMemoryDAO
  private val basicMetrics = Array("saveJar", "saveJobInfo", "saveJobConfig", "saveContextConfig").map {
    name => MetricRegistry.name(classOf[JobDAOMetricsMonitor], name)
  }
  private val fineMetrics = Array("JarSize", "JobInfoSize", "JobConfigSize", "ContextConfigSize").map {
    name => MetricRegistry.name(classOf[JobDAOMetricsMonitor], name)
  }

  before {
    // Resets metrics
    basicMetrics.foreach {
      metricName =>
        MetricsWrapper.getRegistry.remove(metricName)
        MetricsWrapper.getRegistry.meter(metricName)
    }
    fineMetrics.foreach {
     metricName =>
       MetricsWrapper.getRegistry.remove(metricName)
       MetricsWrapper.getRegistry.histogram(metricName)
    }
  }

  describe("JobDAO metrics monitoring") {
    it("should increment basic metric count") {
      val jobDao = JobDAOMetricsMonitor.newInstance(dao, MetricsLevel.BASIC)
      val meters = MetricsWrapper.getRegistry.getMeters
      val histograms = MetricsWrapper.getRegistry.getHistograms

      // Counts of meters should be 0
      basicMetrics.foreach {
        metricName =>
          meters.get(metricName).getCount should equal(0)
      }

      jobDao.saveJar(appName, DateTime.now(), jarBytes)
      jobDao.saveJobInfo(jobInfo)
      jobDao.saveJobConfig(jobId, jobConfig)
      jobDao.saveContextConfig(contextName, contextConfig)

      // Counts of meters should be incremented
      basicMetrics.foreach {
        metricName =>
          meters.get(metricName).getCount should equal(1)
      }

      jobDao.saveJar(appName, DateTime.now(), jarBytes)
      jobDao.saveJobInfo(jobInfo)
      jobDao.saveJobConfig(jobId, jobConfig)
      jobDao.saveContextConfig(contextName, contextConfig)

      // Counts of meters should be incremented again
      basicMetrics.foreach {
        metricName =>
          meters.get(metricName).getCount should equal(2)
      }

      // Fine metrics shouldn't be updated
      fineMetrics.foreach {
        metricName =>
          histograms.get(metricName).getCount should equal(0)
      }
    }

    it("should update fine metrics") {
      val jobDao = JobDAOMetricsMonitor.newInstance(dao, MetricsLevel.FINE)
      val meters = MetricsWrapper.getRegistry.getMeters
      val histograms = MetricsWrapper.getRegistry.getHistograms

      // Fine metrics should be in reset state.
      fineMetrics.foreach {
        metricName =>
          histograms.get(metricName).getCount should equal(0)
      }

      jobDao.saveJar(appName, DateTime.now(), jarBytes)
      jobDao.saveJobInfo(jobInfo)
      jobDao.saveJobConfig(jobId, jobConfig)
      jobDao.saveContextConfig(contextName, contextConfig)

      // Fine metrics count should be updated
      fineMetrics.foreach {
        metricName =>
          histograms.get(metricName).getCount should equal(1)
      }

      jobDao.saveJar(appName, DateTime.now(), jarBytes)
      jobDao.saveJobInfo(jobInfo)
      jobDao.saveJobConfig(jobId, jobConfig)
      jobDao.saveContextConfig(contextName, contextConfig)

      // Fine metrics count should be updated again
      fineMetrics.foreach {
        metricName =>
          histograms.get(metricName).getCount should equal(2)
      }

      // Verify values inside fine metrics
      fineMetrics.foreach {
        metricName => {
          val h = histograms.get(metricName).getSnapshot
          if (metricName.endsWith("JarSize")) {
            h.getMedian should equal(appName.length + jarBytes.length)
          } else if (metricName.endsWith("JobInfoSize")) {
            h.getMedian should equal(contextName.length + classPath.length)
          } else if (metricName.endsWith("JobConfigSize")) {
            val configStr = jobConfig.root().render(ConfigRenderOptions.concise())
            h.getMedian should equal(configStr.length)
          } else if (metricName.endsWith("ContextConfigSize")) {
            val configStr = contextConfig.root().render(ConfigRenderOptions.concise())
            h.getMedian should equal(contextName.length + configStr.length)
          } else {
            fail("Metric " + metricName + " not verified")
          }
        }
      }
    }
  }
}

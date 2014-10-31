package ooyala.common.akka.metrics

import com.codahale.metrics.{Metric, MetricFilter}
import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatest.matchers.ShouldMatchers

class JvmMetricsWrapperSpec extends FunSpec with ShouldMatchers with BeforeAndAfter {
  private val metricsRegistry = MetricsWrapper.getRegistry
  private val metricNamePrefix = "spark.jobserver.jvm"

  def removeJvmMetrics = {
    metricsRegistry.removeMatching(new MetricFilter {
      override def matches(name: String, metric: Metric): Boolean = {
        return name.startsWith(metricNamePrefix)
      }
    })
  }

  before {
    removeJvmMetrics
    JvmMetricsWrapper.registerJvmMetrics(metricsRegistry)
  }

  after {
    removeJvmMetrics
  }

  describe("JvmMetricsWrapper") {
    it("should have valid metrics") {
      val gauges = metricsRegistry.getGauges

      gauges.get(metricNamePrefix + ".thread.count").getValue should not equal(0)
      gauges.get(metricNamePrefix + ".thread.daemon.count").getValue should not equal(0)

      gauges.get(metricNamePrefix + ".heap.committed").getValue should not equal(0)
      gauges.get(metricNamePrefix + ".heap.used").getValue should not equal(0)

      gauges.get(metricNamePrefix + ".non-heap.committed").getValue should not equal(0)
      gauges.get(metricNamePrefix + ".non-heap.used").getValue should not equal(0)
    }
  }
}

package ooyala.common.akka.metrics

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jvm.{GarbageCollectorMetricSet, MemoryUsageGaugeSet, ThreadStatesGaugeSet}
import scala.collection.JavaConverters._

/**
 * JVM metrics wrapper
 */
object JvmMetricsWrapper {
  // JVM metrics Sets
  private val threadMetricSet = new ThreadStatesGaugeSet
  private val memoryMetricSet = new MemoryUsageGaugeSet
  private val gcMetricSet = new GarbageCollectorMetricSet
  private val metricNamePrefix = "spark.jobserver.jvm"

  /**
   * Registers JVM metrics to registry
   *
   * @param registry the registry to register JVM metrics
   */
  def registerJvmMetrics(registry: MetricRegistry) = {
    registry.register(metricNamePrefix + ".thread.count", threadMetricSet.getMetrics.get("count"))
    registry.register(metricNamePrefix + ".thread.daemon.count", threadMetricSet.getMetrics
      .get("daemon.count"))
    registry.register(metricNamePrefix + ".heap.committed", memoryMetricSet.getMetrics
      .get("heap.committed"))
    registry.register(metricNamePrefix + ".heap.used", memoryMetricSet.getMetrics.get("heap.used"))
    registry.register(metricNamePrefix + ".non-heap.committed", memoryMetricSet.getMetrics
      .get("non-heap.committed"))
    registry.register(metricNamePrefix + ".non-heap.used", memoryMetricSet.getMetrics.get("non-heap.used"))

    // Registers gc metrics
    gcMetricSet.getMetrics.asScala.foreach {
      case (name, metric) => {
        registry.register(metricNamePrefix + ".gc." + name, metric)
      }
    }
  }
}

package ooyala.common.akka.metrics

import com.codahale.metrics._
import java.util.concurrent.TimeUnit
import org.coursera.metrics.datadog.DatadogReporter
import org.coursera.metrics.datadog.DatadogReporter.Expansion
import org.coursera.metrics.datadog.transport.HttpTransport
import org.slf4j.LoggerFactory
import scala.util.Try

object MetricsWrapper {
  private val logger = LoggerFactory.getLogger(getClass)
  val registry: MetricRegistry = new MetricRegistry
  private var shutdownHook: Thread = null

  def startDatadogReporter(config: DatadogConfig) = {
    val transportBuilder = new HttpTransport.Builder
    if (config.apiKey.isDefined) {
      transportBuilder.withApiKey(config.apiKey.get)
    }
    val httpTransport = transportBuilder.build

    val datadogReporterBuilder = DatadogReporter.forRegistry(registry)
    if (config.hostName.isDefined) {
      datadogReporterBuilder.withHost(config.hostName.get)
    }
    val datadogReporter = datadogReporterBuilder
      .withTransport(httpTransport)
      .withExpansions(Expansion.ALL)
      .build

    // TODO: Use the cleaner way if possible
    shutdownHook = new Thread {
      override def run {
        datadogReporter.stop
      }
    }

    // Start the reporter and set up shutdown hooks
    datadogReporter.start(config.durationInSeconds, TimeUnit.SECONDS)
    Runtime.getRuntime.addShutdownHook(shutdownHook)

    logger.info("Datadog reporting started.")
  }

  def newGauge[T](klass: Class[_], name: String, metric: => T): Gauge[T] = {
    val metricName = MetricRegistry.name(klass, name)
    val gauge = Try(registry.register(metricName, new Gauge[T] {
      override def getValue(): T = metric
    })).map { g => g } recover { case _ =>
      registry.getGauges.get(metricName)
    }
    gauge.get.asInstanceOf[Gauge[T]]
  }

  def newCounter(klass: Class[_], name: String): Counter =
    registry.counter(MetricRegistry.name(klass, name))

  // TODO: Is this necessary???
  def newCounter(klass: Class[_], name: String, scope: String): Counter =
    registry.counter(MetricRegistry.name(klass, name, scope))

  def newTimer(klass: Class[_], name: String): Timer =
    registry.timer(MetricRegistry.name(klass, name))

  def newHistogram(klass: Class[_], name: String): Histogram =
    registry.histogram(MetricRegistry.name(klass, name))

  def newMeter(klass: Class[_], name: String): Meter =
    registry.meter(MetricRegistry.name(klass, name))

  def getRegistry: MetricRegistry = {
    return registry
  }

  def shutdown = {
    if (shutdownHook != null) {
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
      shutdownHook.run
    }
  }
}
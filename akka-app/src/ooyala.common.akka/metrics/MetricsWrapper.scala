package ooyala.common.akka.metrics

import com.codahale.metrics._

object MetricsWrapper {
  val registry: MetricRegistry = new MetricRegistry
  val reporter: JmxReporter = JmxReporter.forRegistry(registry).build()
  // TODO: Use the cleaner way if possible
  val shutdownHook: Thread = new Thread {
    override def run {
      reporter.stop
    }
  }
  // Start the reporter and set up shutdown hooks
  reporter.start()
  Runtime.getRuntime.addShutdownHook(shutdownHook)

  def newCounter(klass: Class[_], name: String): Counter =
    registry.counter(MetricRegistry.name(klass, name))

  def newCounter(klass: Class[_], name: String, scope: String): Counter =
    registry.counter(MetricRegistry.name(klass, name, scope))

  def getRegistry: MetricRegistry = {
    return registry
  }

  def shutdown = {
    reporter.stop
    Runtime.getRuntime.removeShutdownHook(shutdownHook)
  }
}

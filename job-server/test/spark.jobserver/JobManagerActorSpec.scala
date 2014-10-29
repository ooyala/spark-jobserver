package spark.jobserver

import akka.actor.Props
import com.codahale.metrics.Counter
import ooyala.common.akka.metrics.MetricsWrapper
import spark.jobserver.CommonMessages.{JobErroredOut, JobResult}

class JobManagerActorSpec extends JobManagerSpec {
  import scala.concurrent.duration._

  // Metrics for the job cache
  private val metricCacheHitName = "spark.jobserver.JobCache.cache-hit"
  private val metricCacheMissName = "spark.jobserver.JobCache.cache-miss"
  var metricCacheHit: Counter = _
  var metricCacheMiss: Counter = _

  before {
    // Resets the metrics for the job cache
    MetricsWrapper.getRegistry.remove(metricCacheHitName)
    MetricsWrapper.getRegistry.remove(metricCacheMissName)
    MetricsWrapper.getRegistry.counter(metricCacheHitName)
    MetricsWrapper.getRegistry.counter(metricCacheMissName)

    val counters = MetricsWrapper.getRegistry.getCounters
    metricCacheHit = counters.get(metricCacheHitName)
    metricCacheMiss = counters.get(metricCacheMissName)

    dao = new InMemoryDAO
    manager =
      system.actorOf(JobManagerActor.props(dao, "test", JobManagerSpec.config, false))
  }

  describe("starting jobs") {
    it("jobs should be able to cache RDDs and retrieve them through getPersistentRDDs") {
      manager ! JobManagerActor.Initialize
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "CacheSomethingJob", emptyConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum: Int) = expectMsgClass(classOf[JobResult])

      manager ! JobManagerActor.StartJob("demo", classPrefix + "AccessCacheJob", emptyConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum2: Int) = expectMsgClass(classOf[JobResult])

      sum2 should equal (sum)
    }

    it ("jobs should be able to cache and retrieve RDDs by name") {
      manager ! JobManagerActor.Initialize
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "CacheRddByNameJob", emptyConfig,
        errorEvents ++ syncEvents)
      expectMsgPF(1 second, "Expected a JobResult or JobErroredOut message!") {
        case JobResult(_, sum: Int) => sum should equal (1 + 4 + 9 + 16 + 25)
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
    }
  }

  describe("JobManagerActor JobCache") {
    it("should increase job cache hit count") {
      metricCacheHit.getCount should equal(0)
      metricCacheMiss.getCount should equal(0)

      manager ! JobManagerActor.Initialize
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()

      manager ! JobManagerActor.StartJob("demo", classPrefix + "SimpleObjectJob", emptyConfig,
        errorEvents ++ syncEvents)
      expectMsgClass(classOf[JobResult])
      metricCacheHit.getCount should equal(0)
      // Demo for the first time, misses the job cache
      metricCacheMiss.getCount should equal(1)

      manager ! JobManagerActor.StartJob("demo", classPrefix + "SimpleObjectJob", emptyConfig,
        errorEvents ++ syncEvents)
      expectMsgClass(classOf[JobResult])
      // Demo for the second time, hits the job cache
      metricCacheHit.getCount should equal(1)
      metricCacheMiss.getCount should equal(1)
    }

    it("should increase job cache miss count") {
      metricCacheHit.getCount should equal(0)
      metricCacheMiss.getCount should equal(0)

      manager ! JobManagerActor.Initialize
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()

      manager ! JobManagerActor.StartJob("demo", classPrefix + "SimpleObjectJob", emptyConfig,
        errorEvents ++ syncEvents)
      expectMsgClass(classOf[JobResult])
      metricCacheHit.getCount should equal(0)
      // Demo for the first time, misses the job cache
      metricCacheMiss.getCount should equal(1)

      uploadTestJar("new demo")

      manager ! JobManagerActor.StartJob("new demo", classPrefix + "SimpleObjectJob", emptyConfig,
        errorEvents ++ syncEvents)
      expectMsgClass(classOf[JobResult])
      metricCacheHit.getCount should equal(0)
      // New demo for the first time, misses the job cache again
      metricCacheMiss.getCount should equal(2)
    }
  }
}

package spark.jobserver

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import ooyala.common.akka.metrics.MetricsWrapper
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}


object JobResultCacheSpec {
  val config = ConfigFactory.parseString("""
    spark {
      jobserver.job-result-cache-size = 10
      jobserver.job-result-cache-ttl-seconds = 1
    }
    """)

  val system = ActorSystem("job-result-cache-test", config)
}

class JobResultCacheSpec extends TestKit(JobResultCacheSpec.system) with ImplicitSender
  with FunSpec with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  import CommonMessages._

  override def afterAll() {
    TestKit.shutdownActorSystem(JobResultCacheSpec.system)
  }

  // Metrics for job result cache
  private val metricCacheHitName = "spark.jobserver.JobResultActor.cache-hit"
  private val metricCacheMissName = "spark.jobserver.JobResultActor.cache-miss"
  private val metricCacheRequestName = "spark.jobserver.JobResultActor.cache-request"
  private val metricCacheEvictionName = "spark.jobserver.JobResultActor.cache-eviction"

  before {
    MetricsWrapper.getRegistry.remove(metricCacheHitName)
    MetricsWrapper.getRegistry.remove(metricCacheMissName)
    MetricsWrapper.getRegistry.remove(metricCacheRequestName)
    MetricsWrapper.getRegistry.remove(metricCacheEvictionName)
  }

  describe("JobResultActor cache") {
    it("should increase cache hit count") {
      withActor { actor =>

        actor ! JobResult("jobId", 10)
        actor ! GetJobResult("jobId")
        expectMsg(JobResult("jobId", 10))
        // Hits the job result cache for the first time
        MetricsWrapper.getRegistry.getGauges.get(metricCacheHitName).getValue should equal(1)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheMissName).getValue should equal(0)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheRequestName).getValue should equal(1)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheEvictionName).getValue should equal(0)

        actor ! GetJobResult("jobId")
        expectMsg(JobResult("jobId", 10))
        // Hits the job result cache for the second time
        MetricsWrapper.getRegistry.getGauges.get(metricCacheHitName).getValue should equal(2)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheMissName).getValue should equal(0)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheRequestName).getValue should equal(2)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheEvictionName).getValue should equal(0)
      }
    }

    it("should increase cache miss count") {
      withActor { actor =>
        actor ! JobResult("jobId", 10)
        actor ! GetJobResult("NoJobId")
        expectMsg(NoSuchJobId)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheHitName).getValue should equal(0)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheMissName).getValue should equal(1)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheRequestName).getValue should equal(1)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheEvictionName).getValue should equal(0)

        actor ! GetJobResult("NoJobId")
        expectMsg(NoSuchJobId)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheHitName).getValue should equal(0)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheMissName).getValue should equal(2)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheRequestName).getValue should equal(2)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheEvictionName).getValue should equal(0)
      }
    }

    it("should evict entries according to configured TTL") {
      withActor { actor =>
        val jobResultActor = system.actorOf(Props[JobResultActor])

        (1 to 10).foreach { i =>
          actor ! JobResult(s"jobId_$i", 10)
        }

        MetricsWrapper.getRegistry.getGauges.get(metricCacheEvictionName).getValue should equal(0)

        //from javadoc: requested entries may be evicted on each cache modification, on occasional
        //cache accesses, or on calls to Cache#cleanUp
        Thread.sleep(1500)
        actor ! JobResult("jobId", 10)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheEvictionName).getValue should equal(10)
        actor ! GetJobResult("jobId_1")
        MetricsWrapper.getRegistry.getGauges.get(metricCacheMissName).getValue should equal(1)
      }
    }

    it("should evict entries according to configured cache size") {
      withActor { actor =>
        (1 to 10).foreach { i =>
          actor ! JobResult(s"jobId_$i", 10)
        }

        MetricsWrapper.getRegistry.getGauges.get(metricCacheEvictionName).getValue should equal(0)

        actor ! JobResult("jobId_X", 10)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheEvictionName).getValue should equal(1)

        actor ! JobResult("jobId_Y", 10)
        MetricsWrapper.getRegistry.getGauges.get(metricCacheEvictionName).getValue should equal(2)
      }
    }
  }

  def withActor[T](f: ActorRef => T): T ={
    val actor = TestActorRef(new JobResultActor)
    try {
      f(actor)
    } finally {
      ooyala.common.akka.AkkaTestUtils.shutdownAndWait(actor)
    }
  }
}

package spark.jobserver

import akka.actor.{Props, PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{TestKit, ImplicitSender}
import com.codahale.metrics.Counter
import ooyala.common.akka.metrics.MetricsWrapper
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{FunSpec, BeforeAndAfter, BeforeAndAfterAll}


object JobResultActorSpec {
  val system = ActorSystem("test")
}

class JobResultActorSpec extends TestKit(JobResultActorSpec.system) with ImplicitSender
with FunSpec with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  import CommonMessages._

  override def afterAll() {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(JobResultActorSpec.system)
  }

  // Metrics for job result cache
  private val metricCacheHitName = "spark.jobserver.JobResultActor.cache-hit"
  private val metricCacheMissName = "spark.jobserver.JobResultActor.cache-miss"
  var metricCacheHit: Counter = _
  var metricCacheMiss: Counter = _
  var actor: ActorRef = _

  // Create a new supervisor and FileDAO / working directory with every test, so the state of one test
  // never interferes with the other.
  before {
    // Resets the metrics for the job result cache
    MetricsWrapper.getRegistry.remove(metricCacheHitName)
    MetricsWrapper.getRegistry.remove(metricCacheMissName)
    MetricsWrapper.getRegistry.counter(metricCacheHitName)
    MetricsWrapper.getRegistry.counter(metricCacheMissName)

    val counters = MetricsWrapper.getRegistry.getCounters
    metricCacheHit = counters.get(metricCacheHitName)
    metricCacheMiss = counters.get(metricCacheMissName)

    actor = system.actorOf(Props[JobResultActor])
  }

  after {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(actor)
  }

  describe("JobResultActor") {
    it("should return error if non-existing jobs are asked") {
      actor ! GetJobResult("jobId")
      expectMsg(NoSuchJobId)
    }

    it("should get back existing result") {
      actor ! JobResult("jobId", 10)
      actor ! GetJobResult("jobId")
      expectMsg(JobResult("jobId", 10))
    }

    it("should be informed only once by subscribed result") {
      actor ! Subscribe("jobId", self, Set(classOf[JobResult]))
      actor ! JobResult("jobId", 10)
      expectMsg(JobResult("jobId", 10))

      actor ! JobResult("jobId", 20)
      expectNoMsg()   // shouldn't get it again
    }

    it("should not be informed unsubscribed result") {
      actor ! Subscribe("jobId", self, Set(classOf[JobResult]))
      actor ! Unsubscribe("jobId", self)
      actor ! JobResult("jobId", 10)
      expectNoMsg()
    }

    it("should not publish if do not subscribe to JobResult events") {
      actor ! Subscribe("jobId", self, Set(classOf[JobValidationFailed], classOf[JobErroredOut]))
      actor ! JobResult("jobId", 10)
      expectNoMsg()
    }

    it("should return error if non-existing subscription is unsubscribed") {
      actor ! Unsubscribe("jobId", self)
      expectMsg(NoSuchJobId)
    }
  }

  describe("JobResultActor cache") {
    it("should increase cache hit count") {
      metricCacheHit.getCount should equal(0)
      metricCacheMiss.getCount should equal(0)

      actor ! JobResult("jobId", 10)
      actor ! GetJobResult("jobId")
      expectMsg(JobResult("jobId", 10))
      // Hits the job result cache for the first time
      metricCacheHit.getCount should equal(1)
      metricCacheMiss.getCount should equal(0)

      actor ! GetJobResult("jobId")
      expectMsg(JobResult("jobId", 10))
      // Hits the job result cache for the second time
      metricCacheHit.getCount should equal(2)
      metricCacheMiss.getCount should equal(0)
    }

    it("should increase cache miss count") {
      metricCacheHit.getCount should equal(0)
      metricCacheMiss.getCount should equal(0)

      actor ! JobResult("jobId", 10)
      actor ! GetJobResult("NoJobId")
      expectMsg(NoSuchJobId)
      metricCacheHit.getCount should equal(0)
      // Misses the job result cache for the first time
      metricCacheMiss.getCount should equal(1)

      actor ! GetJobResult("NoJobId")
      expectMsg(NoSuchJobId)
      metricCacheHit.getCount should equal(0)
      // Misses the job result cache for the second time
      metricCacheMiss.getCount should equal(2)
    }
  }
}

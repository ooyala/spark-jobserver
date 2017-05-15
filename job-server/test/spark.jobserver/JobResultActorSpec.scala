package spark.jobserver

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}


object JobResultActorSpec {
  val system = ActorSystem("job-result-actor-test")
}

class JobResultActorSpec extends TestKit(JobResultActorSpec.system) with ImplicitSender
with FunSpec with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  import CommonMessages._

  override def afterAll() {
    TestKit.shutdownActorSystem(JobResultActorSpec.system)
  }

  describe("JobResultActor") {
    it("should return error if non-existing jobs are asked") {
      withActor { actor =>
        actor ! GetJobResult("jobId")
        expectMsg(NoSuchJobId)
      }
    }

    it("should get back existing result") {
      withActor { actor =>
        actor ! JobResult("jobId", 10)
        actor ! GetJobResult("jobId")
        expectMsg(JobResult("jobId", 10))
      }
    }

    it("should be informed only once by subscribed result") {
      withActor { actor =>
        actor ! Subscribe("jobId", self, Set(classOf[JobResult]))
        actor ! JobResult("jobId", 10)
        expectMsg(JobResult("jobId", 10))

        actor ! JobResult("jobId", 20)
        expectNoMsg() // shouldn't get it again
      }
    }

    it("should not be informed unsubscribed result") {
      withActor { actor =>
        actor ! Subscribe("jobId", self, Set(classOf[JobResult]))
        actor ! Unsubscribe("jobId", self)
        actor ! JobResult("jobId", 10)
        expectNoMsg()
      }
    }

    it("should not publish if do not subscribe to JobResult events") {
      withActor { actor =>
        actor ! Subscribe("jobId", self, Set(classOf[JobValidationFailed], classOf[JobErroredOut]))
        actor ! JobResult("jobId", 10)
        expectNoMsg()
      }
    }

    it("should return error if non-existing subscription is unsubscribed") {
      withActor{ actor =>
        actor ! Unsubscribe("jobId", self)
        expectMsg(NoSuchJobId)
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
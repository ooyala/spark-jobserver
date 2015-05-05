package ooyala.common.akka.metrics

import org.scalatest.{FunSpec, Matchers}

class MetricsLevelSpec extends FunSpec with Matchers {

  describe("MetricsLevel") {
    it("should return a valid metric level") {
      MetricsLevel.valueOf(0) should equal(MetricsLevel.NONE)

      MetricsLevel.valueOf(1) should equal(MetricsLevel.BASIC)

      MetricsLevel.valueOf(2) should equal(MetricsLevel.FINE)

      MetricsLevel.valueOf(3) should equal(MetricsLevel.FINER)
    }

    it("should throw exceptions") {
      an [IllegalArgumentException] should be thrownBy {
        MetricsLevel.valueOf(-1)
      }

      an [IllegalArgumentException] should be thrownBy {
        MetricsLevel.valueOf(4)
      }
    }
  }
}

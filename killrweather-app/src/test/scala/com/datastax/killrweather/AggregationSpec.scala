package com.datastax.killrweather

import com.twitter.algebird.{Min, Max}

class AggregationSpec extends AbstractSpec {

  val data = Seq(2.1, 7.4, 0.0, 3.0, 1.6)
  sealed trait TestElementParent
  case object TestElementA extends TestElementParent
  case object TestElementB extends TestElementParent
  case object TestElementC extends TestElementParent

  implicit val testOrdering = Ordering.fromLessThan[TestElementParent]((x, y) => (x, y) match {
    case (TestElementA, TestElementA) => false
    case (TestElementA, _) => true
    case (TestElementB, TestElementB) => false
    case (TestElementB, TestElementA) => false
    case (TestElementB, TestElementC) => true
    case (TestElementC, _) => false
  })
  val data2 = List(TestElementC, TestElementA, TestElementB)

  "MinAggregator" should {
    "produce the minimum value" in {
      val agg = Min.aggregator[Double]
      assert(agg(data) == 0.0)

      val agg2 = Min.aggregator[TestElementParent]
      assert(agg2(data2) == TestElementA)
    }
  }

  "MaxAggregator" should {
    "produce the maximum value" in {
      val agg = Max.aggregator[Double]
      assert(agg(data) == 7.4)

      val agg2 = Max.aggregator[TestElementParent]
      assert(agg2(data2) == TestElementC)
    }
  }
}

package ru.dokwork.easy.kafka.configuration

import java.util

import org.scalatest.{ FreeSpec, Matchers }
import ru.dokwork.easy.kafka.KafkaConsumer.{ AutoCommitStrategy, CommitEveryPollStrategy, NotCommitStrategy }

import scala.concurrent.duration._

class KafkaConfigurationSpec extends FreeSpec with Matchers {

  type K = String
  type V = String

  class TestKafkaConfiguration(override val params: Parameters)
    extends KafkaConfiguration[K, V, TestKafkaConfiguration] {

    /**
     * Builds the [[java.util.Properties Properties]] for kafka with specified parameters.
     */
    override def properties(): util.Properties = super.properties()

    override protected def configure(params: Parameters): TestKafkaConfiguration =
      new TestKafkaConfiguration(params)
  }

  "KafkaConfiguration" - {
    "adds all properties from every parameter when create properties" - {
      "should add bootstrap.servers" in {
        // given:
        val parameters = Parameters.empty + BootstrapServers(Seq("localhost:9090"))
        val conf = new TestKafkaConfiguration(parameters)
        // when:
        val result = conf.properties()
        // then:
        result.get("bootstrap.servers") shouldBe "localhost:9090"
      }
      "should add group.id" in {
        // given:
        val parameters = Parameters.empty + GroupId(Some("test-group"))
        val conf = new TestKafkaConfiguration(parameters)
        // when:
        val result = conf.properties()
        // then:
        result.get("group.id") shouldBe "test-group"
      }
      "should add client.id" in {
        // given:
        val parameters = Parameters.empty + ClientId(Some("test-client"))
        val conf = new TestKafkaConfiguration(parameters)
        // when:
        val result = conf.properties()
        // then:
        result.get("client.id") shouldBe "test-client"
      }
      "should add enable.auto.commit as true and auto.commit.interval.ms for AutoCommitStrategy" in {
        // given:
        val strategy = AutoCommitStrategy(1.second)
        val parameters = Parameters.empty + CommitStrategy(strategy)
        val conf = new TestKafkaConfiguration(parameters)
        // when:
        val result = conf.properties()
        // then:
        result.get("enable.auto.commit") shouldBe "true"
        result.get("auto.commit.interval.ms") shouldBe "1000"
      }
      "should add enable.auto.commit as false for NotCommitStrategy" in {
        // given:
        val strategy = NotCommitStrategy
        val parameters = Parameters.empty + CommitStrategy(strategy)
        val conf = new TestKafkaConfiguration(parameters)
        // when:
        val result = conf.properties()
        // then:
        result.get("enable.auto.commit") shouldBe "false"
      }
      "should add enable.auto.commit as false for CommitEveryPollStrategy" in {
        // given:
        val strategy = CommitEveryPollStrategy
        val parameters = Parameters.empty + CommitStrategy(strategy)
        val conf = new TestKafkaConfiguration(parameters)
        // when:
        val result = conf.properties()
        // then:
        result.get("enable.auto.commit") shouldBe "false"
      }
      "should add all properties" in {
        // given:
        val props = Seq(
          "a" -> "b",
          "1" -> "2"
        )
        val conf = new TestKafkaConfiguration(Parameters.empty)
        // when:
        val result = conf.withProperties(props: _*).properties()
        // then:
        result.get("a") shouldBe "b"
        result.get("1") shouldBe "2"
      }
      "properties should be overridden by values from parameters" in {
        // given:
        val strategy = AutoCommitStrategy(1.second)
        val parameters = Parameters.empty + CommitStrategy(strategy)
        val props = Seq(
          "enable.auto.commit" -> "false",
          "auto.commit.interval.ms" -> "0"
        )
        val conf = new TestKafkaConfiguration(parameters).withProperties(props: _*)
        // when:
        val result = conf.properties()
        // then:
        result.get("enable.auto.commit") shouldBe "true"
        result.get("auto.commit.interval.ms") shouldBe "1000"
      }
    }
  }
}
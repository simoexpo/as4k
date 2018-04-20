import sbt._
import sbt.Keys.{baseDirectory, fork, parallelExecution, scalaSource, resourceDirectory, run}
import sbt.librarymanagement.Configuration
import scala.sys.process._

object Configurations {

  val Benchmark = Configuration.of("Benchmark", "bench") extend Runtime

  lazy val startKafka = taskKey[Unit]("Start kafka in docker")

  lazy val stopKafka = taskKey[Unit]("Stop kafka in docker")

  lazy val benchmarkSettings =
    inConfig(Benchmark)(Defaults.compileSettings) ++
      Seq(
        fork in Benchmark := false,
        parallelExecution in Benchmark := false,
        scalaSource in Benchmark := baseDirectory.value / "src/benchmark/scala",
        resourceDirectory in Benchmark := baseDirectory.value / "src/benchmark/resources",
        startKafka in Benchmark := {
          println("Starting kakfa...")
          "docker-compose up -d" !
        },
        stopKafka in Benchmark := {
          println("Stopping kakfa...")
          "docker-compose down" !
        }
      )

}

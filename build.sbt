lazy val easyKafka = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(
    name := "easy-kafka",
    version := "0.1.0-SNAPSHOT",
    organization := "ru.dokwork",
    scalaVersion := "2.12.4",
    crossScalaVersions := Seq("2.11.11", "2.12.4"),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    libraryDependencies ++= Seq(
      // typesafe:
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "com.typesafe" % "config" % "1.3.1",
      // kafka:
      "org.apache.kafka" % "kafka-clients" % "0.11.0.1" % "provided",
      // tests:
      "org.scalatest" %% "scalatest" % "3.0.0" % "test, it",
      "org.mockito" % "mockito-all" % "1.9.5" % "test, it",
      "ch.qos.logback" % "logback-classic" % "1.1.7" % "test, it",
      "org.apache.kafka" % "kafka-clients" % "0.11.0.1" % "test, it"
    ),
    pomExtra :=
      <developers>
        <developer>
          <id>dokwork</id>
          <name>Vladimir Popov</name>
          <url>http://dokwork.ru</url>
        </developer>
      </developers>
  )
  .settings(
    coverageMinimum := 90,
    coverageFailOnMinimum := true
  )


name := "accumulo-plugin"

organization := "com.schwartech"

version := "1.0-SNAPSHOT"

publishTo := Some(Resolver.file("http://jschwartz73.github.io/play2-accumulo",
  new File("/Users/jeff/dev/myprojects/play2-accumulo.github.com")))

libraryDependencies ++= Seq(
  "org.apache.accumulo" % "accumulo-core" % "1.5.1",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.zookeeper" % "zookeeper" % "3.4.5",
  "log4j" % "log4j" % "1.2.17"
)

play.Project.playJavaSettings

name := "accumulo-plugin"

organization := "com.schwartech"

version := "1.0-SNAPSHOT"

publishTo := Some(Resolver.file("http://schwartech.github.com/m2repo/releases/",
  new File("/Users/jeff/dev/myprojects/schwartech.github.com/m2repo/releases")))

libraryDependencies ++= Seq(
  "org.apache.accumulo" % "accumulo-core" % "1.6.0",
  "org.apache.commons" % "commons-pool2" % "2.2",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.zookeeper" % "zookeeper" % "3.4.5",
  "log4j" % "log4j" % "1.2.17"
)

play.Project.playJavaSettings

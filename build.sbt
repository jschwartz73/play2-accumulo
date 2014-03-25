name := "y"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.apache.accumulo" % "accumulo-core" % "1.5.1"
//  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
//  "org.apache.zookeeper" % "zookeeper" % "3.3.1"
)

play.Project.playJavaSettings

name := "AccumuloSampleApp"

version := "1.1-SNAPSHOT"

libraryDependencies ++= Seq(
  cache,
  "com.schwartech" %% "accumulo-plugin" % "1.1-SNAPSHOT"
)

resolvers += (
  "SchwarTech GitHub Repository" at "http://jschwartz73.github.io/play2-accumulo/"
)

play.Project.playJavaSettings

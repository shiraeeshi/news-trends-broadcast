name := "news-trends-broadcast"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"com.github.catalystcode" %% "streaming-rss-html" % "1.0.2",
	"org.reactivemongo" %% "reactivemongo" % "0.15.0"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

logLevel := Level.Warn

//addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.0.1")


def plugin(m: ModuleID) =
  Defaults.sbtPluginExtra(m, "0.13", "2.10") excludeAll ExclusionRule("org.scala-lang")
libraryDependencies ++= Seq(
  plugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0" excludeAll ExclusionRule("org.scalariform")),
  "com.danieltrinh" %% "scalariform" % "0.1.5"
)

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.0")
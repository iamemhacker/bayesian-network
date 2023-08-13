logLevel := Level.Warn

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

resolvers += "agoda-maven-hkg" at "https://repo-hkg.agodadev.io/agoda-maven"

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.7")

addSbtPlugin("com.agoda.ml" % "sparkdeployplugin" % "2.1.64") //latest is here: https://github.agodadev.io/dse/sparkDeployPlugin/releases

/*addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.12")*/

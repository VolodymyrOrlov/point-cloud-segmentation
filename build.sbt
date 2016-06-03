val geoToolsVersion = "13.6"

val sparkVersion = "1.6.1"

val slf4jVersion = "1.7.14"

lazy val pcSegmentation = (project in file(".")).enablePlugins(JavaAppPackaging).settings (

  name := "pc-segmentation",

  organization := "com.sungevity",

  version := "1.0.0-SNAPSHOT",

  scalaVersion := "2.10.6",

  scalacOptions += "-target:jvm-1.7",

  javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),

  resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools/",

  resolvers += "Artifactory Realm" at "https://sungevity.artifactoryonline.com/sungevity/libs-release-local",

  credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),

  libraryDependencies ++= Seq(
    "com.sungevity.smt" %% "image-toolbox" % "1.0.4",
    "com.sungevity" %% "cma-es-scala" % "1.0.1",
    "com.github.haifengl" % "smile-core" % "1.1.0",
    "com.github.haifengl" % "smile-plot" % "1.1.0",
    "org.geotools" % "gt-main" % geoToolsVersion,
    "org.geotools" % "gt-geotiff" % geoToolsVersion,
    "org.geotools" % "gt-shapefile" % geoToolsVersion,
    "org.geotools" % "gt-process" % geoToolsVersion,
    "org.geotools" % "gt-epsg-hsql" % geoToolsVersion,
    "org.geotools" % "gt-process-raster" % geoToolsVersion,
    "org.geotools" % "gt-process-geometry" % geoToolsVersion,
    "org.geotools" % "gt-process-feature" % geoToolsVersion,
    "org.geotools" % "gt-geojson" % geoToolsVersion,
    "org.geotools.jdbc" % "gt-jdbc-postgis" % geoToolsVersion,
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion excludeAll (ExclusionRule(organization = "org.spark-project.akka")),
    "org.apache.spark" %% "spark-mllib" % sparkVersion excludeAll (ExclusionRule(organization = "org.spark-project.akka")),
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  ),

  assemblyMergeStrategy in assembly := {
    case m if m.startsWith("META-INF/services/") => MergeStrategy.concat
    case m if m.startsWith("META-INF/maven/") => MergeStrategy.discard
    case m if m.endsWith(".DSA") => MergeStrategy.discard
    case m if m.endsWith(".RSA") => MergeStrategy.discard
    case m if m.endsWith(".SF") => MergeStrategy.discard
    case m if Seq("MANIFEST.MF", "LICENSE.txt", "NOTICE.txt").exists("META-INF/" + _ == m) => MergeStrategy.discard
    case m if m == "reference.conf" => MergeStrategy.concat
    case _         => MergeStrategy.first
  },

  mappings in Universal <<= (mappings in Universal, assembly in Compile) map { (mappings, fatJar) =>
    val filtered = mappings filter { case (file, name) =>  ! name.endsWith(".jar") }
    filtered :+ (fatJar -> ("lib/" + fatJar.getName))
  },

  mappings in Universal += {
    (baseDirectory in Compile).value / "application.conf.template" -> "conf/application.conf"
  },


  scriptClasspath := Seq( (assemblyJarName in assembly).value ),


  mainClass in Compile := Some("com.sungevity.smt.faces.Main")

)

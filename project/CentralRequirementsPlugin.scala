import sbt._, Keys._
import xerial.sbt.Sonatype.autoImport.sonatypeProfileName

object CentralRequirementsPlugin extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = verizon.build.RigPlugin
  override lazy val projectSettings = Seq(
    sonatypeProfileName := "com.github.yilinwei",
    developers += Developer("yilinwei", "Yilin Wei", "", url("http://github.com/yilinwei")),
    licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    homepage := Some(url("https://github.com/yilinwei/fs2-hazelcast")),
    scmInfo := Some(ScmInfo(url("https://github.com/yilinwei/fs2-hazelcast"),
      "scm:git:git@github.com:yilinwei/fs2-hazelcast.git"))
  )
}

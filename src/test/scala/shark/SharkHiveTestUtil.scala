package shark

import java.io.File

trait SharkHiveTestUtil {
  /**
   * Get the directory to store test data in. If not specified as a system property, detects
   * the directory automatically (e.g. when running in an IDE).
   */
  def getTestDir: String =
    (scala.Option(System.getProperty("test.dir")) match {
      case None => {
        val classDir = getClass.getResource("").getPath
        var targetDir = new File(classDir)
        while (targetDir.getName != "target") {
          val parentDir = targetDir.getParentFile
          assert(!parentDir.equals(targetDir))
          targetDir = parentDir
        }
        new File(targetDir, "testing")
      }
      case Some(testDir) => new File(testDir)
    }).getAbsolutePath

  /** Set the Hive test directory */
  def setHiveTestDir() {
    System.setProperty("hive.test.dir", getTestDir)
  }
}

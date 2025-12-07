package testsupport

import java.nio.file.{Files, Path, Paths}

trait TempFsHelper {
  def tempDir(name: String): Path = {
    val base = Paths.get(System.getProperty("java.io.tmpdir"), "octopufs-tests")
    Files.createDirectories(base)
    val p = base.resolve(name + "-" + System.nanoTime())
    Files.createDirectories(p)
    p
  }
}

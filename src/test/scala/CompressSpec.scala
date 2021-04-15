import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream
import fs2.io.file.Files

import java.nio.file.Paths
class CompressSpec extends munit.FunSuite {
  test("Compress"){
    Files[IO]
      .readAll(Paths.get("/home/nacho/Documents/test/default/00.txt"),4096)
      .debug()
      .compile
      .drain
      .unsafeRunSync()
  }

}

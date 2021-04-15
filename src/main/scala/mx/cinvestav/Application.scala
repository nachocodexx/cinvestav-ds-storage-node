package mx.cinvestav

import cats.implicits._
import cats.data.Kleisli
import cats.effect.{ExitCode, IO, IOApp}
import mx.cinvestav.config.DefaultConfig
import org.http4s.{HttpRoutes, Request, Response}
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import pureconfig.ConfigSource
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import pureconfig._
import pureconfig.generic.auto._
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.http4s.circe._
import org.http4s.multipart.Multipart

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.global
import fs2.Stream
import fs2.io.file.Files
import fs2.io.writeOutputStream
import fs2.concurrent.Topic

import java.nio.file.{OpenOption, Path, Paths, StandardOpenOption, Files => FFF}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.http4s.circe.CirceEntityCodec._
import sun.awt.shell.ShellFolder

import java.io.{File, FileOutputStream}
import java.util.concurrent.{Executor, ExecutorService, Executors}
import scala.concurrent.ExecutionContext

object Application extends IOApp{
  case class NodeHealthInfo(nodeId:String,createdAt:Long, status:Int,message:String)
  def services()(implicit  C:DefaultConfig): HttpRoutes[IO] = HttpRoutes.of[IO]{
    case req@POST -> Root =>
      req.decode[Multipart[IO]]{ m=>
        val parts = m.parts
        Stream.emits(parts)
          .covary[IO]
          .flatMap{ part =>
            val filename = part.filename.getOrElse("sample")
            val output =  FFF.newOutputStream(Paths.get(s"${C.storagePath}/$filename.gz"),StandardOpenOption.CREATE_NEW)
            val _cOut   = new CompressorStreamFactory()
              .createCompressorOutputStream(CompressorStreamFactory.getGzip,output)
            val cOut = writeOutputStream[IO](_cOut.pure[IO])
            part.body
              .through(cOut)
              .debug()
              .onComplete(Stream.eval(IO.println(s"$filename compress and saved")))
          }
          .compile
          .drain
          .flatMap(_=>Ok("YES"))
      }
    case GET -> Root / "read"/ filename=>
      val filePathStr  = s"${C.storagePath}/$filename"
      val file = new File(filePathStr)
        val fileExists = file.exists()
      if (!fileExists)
        NotFound()
      else {
        val fileStream = Files[IO].readAll(Paths.get(filePathStr), 4096)
        Ok(fileStream)
      }

    case GET -> Root / "health-check" =>
      IO.realTime.flatMap{ time=>
        Ok(NodeHealthInfo(C.nodeId,time.toSeconds,1,"I'm okay :)"))
      }
  }
  def httpApp()(implicit C:DefaultConfig): Kleisli[IO, Request[IO], Response[IO]] =
    Router(s"/${C.nodeId}" -> services).orNotFound
  def serverBuilder()(implicit C:DefaultConfig): BlazeServerBuilder[IO] =
    BlazeServerBuilder[IO](global)
      .withIdleTimeout(1 minute)
    .bindHttp(C.port, C.host).withHttpApp(httpApp)


  override def run(args: List[String]): IO[ExitCode] = {
    val config = ConfigSource.default.load[DefaultConfig]
    config match {
      case Left(value) =>
        println(value)
        IO.unit.as(ExitCode.Error)
      case Right(cfg) =>
        println(cfg)
        serverBuilder()(cfg)
          .resource
        .use(_=>IO.never)
    }
  }
}

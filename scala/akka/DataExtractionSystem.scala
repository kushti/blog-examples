package blogexamples.akka

import akka.actor.{ActorSystem, ReceiveTimeout, Props, Actor}
import concurrent.ExecutionContext
import scala.concurrent.duration._


case class Property(name:String)
case class ExtractionResult(value: Either[Throwable, List[Property]])

trait BasePropertyExtractor {
  def extractData:ExtractionResult
  def label:String
}

class SiteAExtractor extends BasePropertyExtractor{
  override def extractData = ExtractionResult(Right(List[Property](Property("Nice beachside boongalow"))))
  override def label = "SiteAExtractor"
}

class SiteBExtractor extends BasePropertyExtractor{
  override def extractData = ExtractionResult(Right(List[Property](Property("Awesome apartments"))))
  override def label = "SiteBExtractor"
}

class SiteCExtractor extends BasePropertyExtractor{
  override def extractData = ExtractionResult(Left(new Exception("XML parsing failed")))
  override def label = "SiteCExtractor"
}



case class ExtractionCommand(extractor:BasePropertyExtractor)
object StartExtraction



class ExtractingActor extends Actor with akka.actor.ActorLogging {
  override def receive = {
    case ExtractionCommand(extractor:BasePropertyExtractor) =>
      println("Going to extract data by "+extractor.label)
      sender ! extractor.extractData
  }
}


class DbWriterActor extends Actor with akka.actor.ActorLogging {
  override def receive = {
    case p: Property  => println(p)
  }
}

class ScatterGatherer extends Actor with akka.actor.ActorLogging {
  context.setReceiveTimeout(29 seconds)

  private val actorsCommands = Map(
    context.actorOf(Props[ExtractingActor]) -> ExtractionCommand(new SiteAExtractor),
    context.actorOf(Props[ExtractingActor]) -> ExtractionCommand(new SiteBExtractor),
    context.actorOf(Props[ExtractingActor]) -> ExtractionCommand(new SiteCExtractor)
  )

  private val dbWriterActor = context.actorOf(Props[DbWriterActor])

  override def receive = {
    case StartExtraction =>
      actorsCommands foreach {
        case (actor, command) => actor ! command
      }

    case result: ExtractionResult => result.value match{
      case Left(t:Throwable) => log.warning("Exception found instead of result: " + t)
      case Right(l:List[Property]) => l foreach {dbWriterActor ! _}
    }

    case ReceiveTimeout =>
      context.stop(self)
      actorsCommands foreach {_ => context.stop(_)}
      context.stop(dbWriterActor)
  }
}


object RealEstateExtractionLauncher extends App {
  import ExecutionContext.Implicits.global
  val system = ActorSystem("RealEstateExample")

  system.scheduler.schedule(0 seconds, 30 seconds){
    val listeningActor = system.actorOf(Props[ScatterGatherer])
    listeningActor ! StartExtraction
  }
}
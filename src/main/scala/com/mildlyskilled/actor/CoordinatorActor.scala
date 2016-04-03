package com.mildlyskilled.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.mildlyskilled.{Image, Color, Scene}
import com.mildlyskilled.protocol.{CoordinatorProtocol, ListenerProtocol, TracerProtocol}
import akka.routing._


class CoordinatorActor(outputFile: String, image: Image, listener: ActorRef) extends Actor with ActorLogging {
  private var waiting = image.width * image.height
  private var currentXAxis = -1
  private var currentYAxis = 0
  private var scene: Scene = null
  val nrOfWorkers = 150
  log.info("Number of TracerActors: " + nrOfWorkers.toString())


  var router = {
    val routees = Vector.fill(nrOfWorkers) {
      val r = context.actorOf(Props[TracerActor])
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  def receive = {
    case CoordinatorProtocol.TraceImage(newScene) =>
      scene = newScene
      for (x <- 0 until nrOfWorkers) {
        var (x, y) = nextPixel()
        router.route(TracerProtocol.TracePixel(scene, image.width, image.height, x, y), self)
      }
    case CoordinatorProtocol.Set(x: Int, y: Int, c: Color) =>
      image(x, y) = c
      waiting -= 1

      if (waiting > 0) {
        var (x, y) = nextPixel()
        if (y < image.height) {
          router.route(TracerProtocol.TracePixel(scene, image.width, image.height, x, y), self)
        }
      } else if (waiting == 0) {
        listener ! ListenerProtocol.Finish(image, outputFile)
      }
  }

  private def nextPixel(): (Int, Int) = {
    currentXAxis += 1
    if (currentXAxis == image.width) {
      currentXAxis = 0
      currentYAxis += 1
    }
    (currentXAxis, currentYAxis)
  }

}

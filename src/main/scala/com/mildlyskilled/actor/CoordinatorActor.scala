package com.mildlyskilled.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import com.mildlyskilled.{Image, Color, Scene}
import com.mildlyskilled.protocol.{CoordinatorProtocol, ListenerProtocol, TracerProtocol}
import akka.routing.{Router, RoundRobinRoutingLogic, ActorRefRoutee}

/**
 * Delegates work to TracerActors, monitors progress and inform the Listener when complete.
 * 
 * @param outputFile the path for printing the final image.
 * @param image the Image file.
 * @param listener reference to contact the listener.
 * @param scene describes the image to be ray-traced.
 */
class CoordinatorActor(outputFile: String, image: Image, listener: ActorRef, scene: Scene) 
  extends Actor with ActorLogging {
  
  //waiting monitors the number of pixels which need to be processed. The starting value is the
  //total.
  private var waiting = image.width * image.height
  
  //Monitors which row was last dispatched for processing. Initially set to -1 because the first
  //row should be 0 and each call to this variable hould increment it by 1.
  private var currentRow = -1
  
  //The number of TracerActors that the Coordinator will create.
  //Utilise the number of availabe cores.
  val nrOfWorkers = Runtime.getRuntime().availableProcessors()
  log.info("Number of TracerActors: " + nrOfWorkers.toString())

  //Creates a router to manage the TracerActors.
  //akka.actor.deployment
  var router = {
    val routees = Vector.fill(nrOfWorkers) {
      val r = context.actorOf(Props[TracerActor])
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees) 
  }

  def receive = {
    
    //TraceImage message begins processing.
    case CoordinatorProtocol.TraceImage =>
      //Delegates the first portion of work for each actor.
      for (i <- 0 until nrOfWorkers) {
        //Calls nextRow which increments currentRow. If nextRow returns true then work is delegated.
        //If nextRow returns false, then no work is delegated.
        if (nextRow()) {
          router.route(TracerProtocol.TracePixels(scene, image.width, image.height, currentRow), self)
        }
      }
    
    //A Set message inform the coordinator about the color of a pixel. The coordinator applies this colour
    //to the image.
    case CoordinatorProtocol.Set(x: Int, y: Int, c: Color) =>
      //Applies the colour to the image where x and y indicate coordinates and c is a Color object.
     // log.info(x.toString(), y.toString())
      image(x, y) = c
      //Decrements one from waiting.
      waiting -= 1
      
    //RequestMoreWork messages are sent by TracerActors when they run out of work.
    case CoordinatorProtocol.RequestMoreWork =>
      //Calls nextRow which increments currentRow. If nextRow returns true then work is delegated.
      //If nextRow returns false, then no work is delegated.
      if (nextRow()) {
         router.route(TracerProtocol.TracePixels(scene, image.width, image.height, currentRow), self)
      //Checks whether waiting equals 0. When waiting is 0, then all processing is complete.
      //The coordinator informs the Listener using a Finish message..
      } else if (waiting == 0) {
        listener ! ListenerProtocol.Finish(image, outputFile)
      } else {
        log.info(sender.path + " has run out of work.")
      }
  }
  
  /**
   * Increments the currentRow counter by 1. Returns true or false depending on whether the row is
   * in bounds of the image file.
   * 
   * @return true if current row is in bounds.
   */
  private def nextRow(): Boolean = {
    var result = false
    currentRow += 1
    if (currentRow < image.height) {
      result = true
    }
    result
  }
}

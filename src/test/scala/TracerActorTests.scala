package sample.akka.testkit

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, MustMatchers, WordSpec, WordSpecLike, BeforeAndAfterEach}
import com.mildlyskilled._
import com.mildlyskilled.actor.{TracerActor, _}
import com.mildlyskilled.protocol._

import scala.concurrent.duration._

class TracerActorTests extends TestKit(ActorSystem("testSystem"))
  with WordSpecLike
  with MustMatchers
  with ImplicitSender
  with BeforeAndAfterAll
  with BeforeAndAfterEach {


  val infile = "src/main/resources/input.dat"
  val scene = FileReader.parse(infile)
  val row = 0
  val image = new Image(Trace.Width, Trace.Height)
  val color = Color.black
  val outfile = "output.png"
  val myListener = TestActorRef[Listener]
  val traceActorRef = TestActorRef[TracerActor]
  val coordActorRef = TestActorRef(Props(new CoordinatorActor(outfile, image, myListener, scene)))
  val listenerActorRef = TestActorRef[Listener]

  override def afterEach() {
    traceActorRef.stop()
    listenerActorRef.stop()
    coordActorRef.stop()
    myListener.stop()
    shutdown()
  }

  "A Tracer actor" must {
    // Creation of the TestActorRef
    "send a set message" in {
      within(1000 millis) {
        // This call is synchronous. The actor receive() method will be called in the current thread
        traceActorRef ! TracerProtocol.TracePixels(scene, Trace.Width, Trace.Height, row)
        // With actorRef.underlyingActor, we can access the SimpleActor instance created by Akka
        expectMsg(CoordinatorProtocol.Set(0, 0, color))
      }
    }
    "send 800 set messages" in {
      traceActorRef ! TracerProtocol.TracePixels(scene, Trace.Width, Trace.Height, row)
      // With actorRef.underlyingActor, we can access the SimpleActor instance created by Akka
      receiveN(800)
    }
  }

  "A Coordinator actor" must {
    // Creation of the TestActorRef

    "send a set message2" in {
      within(1000 millis) {
        // This call is synchronous. The actor receive() method will be called in the current thread
        coordActorRef ! CoordinatorProtocol.TraceImage
        // With actorRef.underlyingActor, we can access the SimpleActor instance created by Akka
        expectNoMsg()
      }
    }
    "send a set message3" in {
      within(1000 millis) {
        // This call is synchronous. The actor receive() method will be called in the current thread
        coordActorRef ! CoordinatorProtocol.RequestMoreWork
        // With actorRef.underlyingActor, we can access the SimpleActor instance created by Akka
        expectNoMsg()
      }
    }
    "send a set message4" in {
      within(1000 millis) {
        // This call is synchronous. The actor receive() method will be called in the current thread
        coordActorRef ! CoordinatorProtocol.Set(0, 0, Color.black)
        // With actorRef.underlyingActor, we can access the SimpleActor instance created by Akka
        expectNoMsg()
      }
    }
  }

  "A Listener actor must" must {
    // Creation of the TestActorRef
    "send a set message" in {
      within(1000 millis) {
        // This call is synchronous. The actor receive() method will be called in the current thread
        listenerActorRef ! ListenerProtocol.Finish(image, outfile)
        // With actorRef.underlyingActor, we can access the SimpleActor instance created by Akka
        expectNoMsg()
      }
    }
  }

}
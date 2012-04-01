package spark

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeMap

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.collection.mutable.HashSet

sealed trait UpdatedProgressSharerMessage
case class SlaveLocation(slaveHost: String, slavePort: Int) extends UpdatedProgressSharerMessage
case class UpdatedProgressMessageToMaster(message: UpdatedProgressMasterMessage[_,_]) extends UpdatedProgressSharerMessage
case class UpdatedProgressDiffToSlave(diff: UpdatedProgressDiff[_,_]) extends UpdatedProgressSharerMessage
case object StopUpdatedProgressSharer extends UpdatedProgressSharerMessage

class UpdatedProgressMasterSenderActor(_serverUris: ArrayBuffer[(String, Int)]) extends DaemonActor with Logging {
    var diffsToSend = new HashMap[Long, UpdatedProgressDiff[_,_]]
    var serverUris = _serverUris

    var diffsWillSend = diffsToSend.clone()
    var serverWillSend = 0
    var serverFirstSent = 0

    var mustStop = false

    def act() {
        val port = System.getProperty("spark.master.port").toInt
        RemoteActor.alive(port)
        RemoteActor.register('UpdatedProgressMasterSender, self)
        logInfo("Registered actor on port " + port)

        while(true) {
          diffsToSend.synchronized {
            if (diffsToSend.size > 0) {
              for ((key, value) <- diffsToSend) {
                diffsWillSend.update(key, value)
              }
              // will send all diffs, so clear
              diffsToSend.clear()

              // todo: jperla: this is a tiny bit bad if there are lots of servers 
              // and lots of different shared variables updated a lot
              // not our applications though

              // keep track of this so we can stop when we loop back to this server
              serverFirstSent = serverWillSend
              println("new diff to send, new first server: " + serverFirstSent + "/" + serverUris.size)
            }
          }

          var host = ""
          var port = 0

          serverUris.synchronized {
            if (serverUris.size > 0) {
              val pair = serverUris(serverWillSend)
              host = pair._1
              port = pair._2
              println("sending to server " + serverWillSend + " @ " + host + ":" + port)
            }
          }

          if (diffsWillSend.size > 0 && port > 0) {
            //println("diffs to send > 0!")
            // still diffs to send, and still new servers to send to

            // todo: jperla: this sets up and tears down a TCP connection, slow!
            // improve this by using keeping connections around, or using UDP or something
            // Q: can master actor handle keeping 800 connections open to slaves?

            // iterate over serveruris, and send messages
            var slave = RemoteActor.select(Node(host, port), 'UpdatedProgressSharerSlave)
            // send all the diffs to this one server
            for(diff <- diffsWillSend.values) {
	      
              println("sending actual diff id: " + diff.id)
              slave ! UpdatedProgressDiffToSlave(diff)
            }
            slave = null
          } else {
            // if no diffs; let them accumulate for 50 ms
            Thread.sleep(50)
          }

          // next loop, do the next server
          // do this outside the loop so that it sends to different server first every time
          // loop back around to 0 when hit the end
          // assumption: serverUris never shrinks
          if (serverUris.size > 0) {
            serverWillSend = (serverWillSend + 1) % serverUris.size
          } else {
            serverWillSend = 0
          }

          if (serverWillSend == serverFirstSent && diffsWillSend.size > 0) {
              // looped back to first server, so clear the diffs we would send
              println("sent to all servers, done")
              diffsWillSend.clear() 
          }

          if (mustStop) {
            println("has must stop true")
            exit()
          }
        }
    }

    def stop() {
      println("stop() called")
      mustStop = true
    }
}


class UpdatedProgressMasterReceiverActor(masterSenderActor: UpdatedProgressMasterSenderActor) extends DaemonActor with Logging {
  val uniqueServers = new HashSet[String]

  def act() {
    val port = System.getProperty("spark.master.port").toInt
    RemoteActor.alive(port)
    RemoteActor.register('UpdatedProgressMasterReceiver, self)
    logInfo("Registered actor on port " + port)

    
    loop {
      react {
        case SlaveLocation(slaveHost: String, slavePort: Int) =>
          println("Asked to register new slave at " + slaveHost + ":" + slavePort)
          if (!uniqueServers.contains(slaveHost)) {
            uniqueServers.add(slaveHost)
            masterSenderActor.serverUris.synchronized {
              masterSenderActor.serverUris.append((slaveHost, slavePort))
            }
          }
          reply('OK)
        case UpdatedProgressMessageToMaster(newVar: UpdatedProgressMasterMessage[_,_]) =>
          println("Received message @ master from " + sender + " " + newVar.id + ":" + newVar)
          updateAndSendIfNeeded(newVar.id, newVar)
        case StopUpdatedProgressSharer =>
          println("received StopUpdatedProgressSharer")
          masterSenderActor.stop()
          reply('OK)
          exit()
      }
    }
  }

  def updateAndSendIfNeeded[G,T] (varId : Long, message : UpdatedProgressMasterMessage[G,T]) {
        var oldVar = UpdatedProgressVars.originals(varId).asInstanceOf[UpdatedProgress[G,T]]
        var updateToSend = oldVar.masterAggregate(message.message)

        if (updateToSend != null) {
            UpdatedProgressVars.applyDiff(updateToSend) // need to apply to all local Master threads

            masterSenderActor.diffsToSend.synchronized {
                masterSenderActor.diffsToSend.put(updateToSend.id, updateToSend)
            }

            println("updateToSend is not null")
        } else {
            println("updateToSend is null")
        }
  }
}

class UpdatedProgressSharerSlaveActor()
extends DaemonActor with Logging {

  def act() {
    val slavePort = System.getProperty("spark.slave.port").toInt
    RemoteActor.alive(slavePort)
    RemoteActor.register('UpdatedProgressSharerSlave, self)
    logInfo("Registered actor on port " + slavePort)

    loop {
      react {
        case UpdatedProgressDiffToSlave(diff: UpdatedProgressDiff[_,_]) =>
          println("testing ws value after receiving" + diff)
          UpdatedProgressVars.applyDiff(diff)
        case StopUpdatedProgressSharer =>
          reply('OK)
          exit()
      }
    }
  }
}

class UpdatedProgressSharer(isMaster: Boolean) extends Logging {
  var masterReceiverActor: AbstractActor = null
  var slaveActor: AbstractActor = null
  var masterSenderActor: AbstractActor = null

  private var serverUris = new ArrayBuffer[(String, Int)]

  println("is master: " + isMaster)
  
  if (isMaster) {
    val masterSenderActor = new UpdatedProgressMasterSenderActor(serverUris)
    masterSenderActor.start()

    val sharer = new UpdatedProgressMasterReceiverActor(masterSenderActor)
    sharer.start()
    masterReceiverActor = sharer
  } else {
    val host = System.getProperty("spark.master.host")
    val port = System.getProperty("spark.master.port").toInt
    masterReceiverActor = RemoteActor.select(Node(host, port), 'UpdatedProgressMasterReceiver)

    val slaveActor = new UpdatedProgressSharerSlaveActor()
    slaveActor.start()

    val slavePort = System.getProperty("spark.slave.port").toInt
    // todo: jperla: can we get host and port name from val slaveActor itself?
    masterReceiverActor !?  SlaveLocation(Utils.localIpAddress, slavePort)
  }

  def sendUpdatedProgressMasterMessage[G,T](p: UpdatedProgressMasterMessage[G,T]) {
    println("sending updated progress master message")
    masterReceiverActor ! UpdatedProgressMessageToMaster(p)
  }
  
  def stop() {
    masterReceiverActor !? StopUpdatedProgressSharer
    serverUris.clear()
    masterReceiverActor = null

    slaveActor !? StopUpdatedProgressSharer
    slaveActor = null
  }
}

package spark

import scala.collection.mutable.HashMap

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.collection.mutable.HashSet

sealed trait UpdatedProgressSharerMessage
case class SlaveLocation(slaveHost: String, slavePort: Int) extends UpdatedProgressSharerMessage
case class UpdatedProgressMessageToMaster(message: UpdatedProgressMasterMessage[_,_]) extends UpdatedProgressSharerMessage
case class UpdatedProgressDiffToSlave(diff: UpdatedProgressDiff[_,_]) extends UpdatedProgressSharerMessage
case object StopUpdatedProgressSharer extends UpdatedProgressSharerMessage

class UpdatedProgressMasterSenderActor(_serverUris: HashMap[String, Int]) extends DaemonActor with Logging {
    var diffsToSend = new HashMap[Long, UpdatedProgressDiff[_,_]]
    var serverUris = _serverUris

    def act() {
        val port = System.getProperty("spark.master.port").toInt
        RemoteActor.alive(port)
        RemoteActor.register('UpdatedProgressMasterSender, self)
        logInfo("Registered actor on port " + port)

        while(true) {
          diffsToSend.synchronized {
            for(diff <- diffsToSend.values) {
              // todo: jperla: this sets up and tears down a TCP connection, slow!
              // improve this by using keeping connections around, or using UDP or something
              // iterate over serveruris, and send messages
              serverUris.synchronized {
                for ((host,port) <- serverUris) {
                    var slave = RemoteActor.select(Node(host, port), 'UpdatedProgressSharerSlave)
                    slave ! UpdatedProgressDiffToSlave(diff)
                    slave = null
                }
              }
            }
            // sent all diffs, so clear
            diffsToSend.clear()
          }
          
          // send to slaves every 50 milliseconds; won't send more than 20 a second to slaves
          // todo: jperla: should probably check immediately if previous step took 50+ milliseconds
          // todo: jperla: how do we time this?
          Thread.sleep(50)
        }
    }
}


class UpdatedProgressMasterReceiverActor(masterSenderActor: UpdatedProgressMasterSenderActor) extends DaemonActor with Logging {

  def act() {
    val port = System.getProperty("spark.master.port").toInt
    RemoteActor.alive(port)
    RemoteActor.register('UpdatedProgressMasterReceiver, self)
    logInfo("Registered actor on port " + port)

    
    loop {
      react {
        case SlaveLocation(slaveHost: String, slavePort: Int) =>
          println("Asked to register new slave at " + slaveHost + ":" + slavePort)
          masterSenderActor.serverUris.synchronized {
            masterSenderActor.serverUris.put(slaveHost, slavePort)
          }
          reply('OK)
        case UpdatedProgressMessageToMaster(newVar: UpdatedProgressMasterMessage[_,_]) =>
          println("Received message @ master from " + sender + " " + newVar.id + ":" + newVar)
          updateAndSendIfNeeded(newVar.id, newVar)
        case StopUpdatedProgressSharer =>
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

  private var serverUris = new HashMap[String, Int]

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

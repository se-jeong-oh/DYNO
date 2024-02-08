/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * deprecated imports here
 * import java.io.File
 * import java.nio.file.Paths
 */

package com.nvidia.spark.rapids

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net._

import scala.collection.mutable.{ListBuffer, Map}

import ai.rapids.cudf._
import com.nvidia.spark.rapids.shims.{GlobalLimitShims, SparkShimImpl}
import util.control.Breaks._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, GetStructField, WindowFrame, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti, LeftSemi}
import org.apache.spark.sql.execution.{ExpandExec, FileSourceScanExec, FilterExec, GlobalLimitExec, LocalLimitExec, ProjectExec, SortExec, SparkPlan, TakeOrderedAndProjectExec, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

package object idpackage {
  var id: Int = 0
  var optimizations: Seq[Optimization] = new ListBuffer[Optimization]()
  var currentMBSize: Double = 0.0
}

/**
 * Optimizer that can operate on a physical query plan.
 */
trait ModiOptimizer {

  /**
   * Apply optimizations to a query plan.
   *
   * @param conf Rapids configuration
   * @param plan The plan to optimize
   * @return A list of optimizations that were applied
   */
  def optimize(conf: RapidsConf, plan: SparkPlanMeta[SparkPlan]): Seq[Optimization]
}

/**
 * Experimental cost-based optimizer that aims to avoid moving sections of the plan to the GPU when
 * it would be better to keep that part of the plan on the CPU. For example, we don't want to move
 * data to the GPU just for a trivial projection and then have to move data back to the CPU on the
 * next step.
 */
class ModiCostBasedOptimizer(conf: RapidsConf) extends ModiOptimizer with Logging {

  /**
   * Walk the plan and determine CPU and GPU costs for each operator and then make decisions
   * about whether operators should run on CPU or GPU.
   *
   * @param conf Rapids configuration
   * @param plan The plan to optimize
   * @return A list of optimizations that were applied
   */

    // jgh: for LMStream
    // --------------------variables for LMStream--------------------------
    // the intention is to make the cost model pluggable since we are probably going to need to
    // experiment a fair bit with this part
    // private val costModel: CostModel = _
    private val costModel = new DefaultCostModel(conf)

    // Declare private var to change currentBatchSize.
    // private var currentBatchSize: Double = currentBatchSizeOrigin
    // Since a single CPU core and GPU connection is in charge of one data partition,
    // we use currentBatchSize as the size of a single data partition.
    private var currentBatchSize: Double = _

    // Save max estimation of output size of each operator.
    private var previousOutputSize: Double = 0.0

    // jgh: originally fetched from spark.SparkPlan. but we don't know these initial value.
    // so, we set initial value to 150KB (as it says on the paper)
    private val inflectionPointOfBatchSize: Double = 150000.0

    // jgh: i think this value is number of dataset in Batch.
    private var sizeOfBatchedFileSource: Double = 0.0

    // jgh: size of Data Partition in MB
    private var numPartitions: Int = -1

    // jgh: Since sparkRapids allows 1:1=gpu:executor, this value is equal to number of executor.
    private val numTotalGpuDevices: Int = 1

    private var currentBatchSizeOrigin: Double = 0
    //-----------------------------------------------------------------------

  def optimize(conf: RapidsConf, plan: SparkPlanMeta[SparkPlan]): Seq[Optimization] = {
    logTrace("CBO optimizing plan")
    // val cpuCostModel = new ModiCpuCostModel(conf)
    // val gpuCostModel = new ModiGpuCostModel(conf)
    val optimizations = new ListBuffer[Optimization]()

    // jgh start
    val currentMBSize = SparkPlan.getCurrentMBSize
    if (currentMBSize == idpackage.currentMBSize) {
      return idpackage.optimizations
    }
    else {
      idpackage.currentMBSize = currentMBSize
    }
//  val totalMBSize = SparkPlan.getTotalMBSize
    val numDSOfMB = SparkPlan.getNumDSOfMB
    numPartitions = SparkPlan.getNumPartitions // for LMStream
    sizeOfBatchedFileSource = numDSOfMB // for LMStream
    currentBatchSizeOrigin = currentMBSize
    currentBatchSize = currentBatchSizeOrigin / numPartitions
    // jgh end

    // sej start
    // [TODO] Get SparkPlan Operation Data (List of Operation Name) 
    // plan.wrapped.getClass.getSimpleName

    // make request pipe writer
    // val requestPath = "/home/sej/data/pipe/request"
    val serverAddress = InetAddress.getByName("localhost")
    val serverPort = 13456
    val socket = new Socket(serverAddress, serverPort)
    val writer = new PrintWriter(socket.getOutputStream, true)
    // val writer = new PrintWriter(new FileWriter(requestPath, true))
    // val requestPath = Paths.get("/home/sej/data/pipe/request")
    // val writer = new PrintWriter(new File(requestPath.toString), true)

    // recursively walk through plan, and write request data to request pipe.
    // logInfo("[DyOp_sej] recursivelyRequest Starts.")
    val idOpMapOrg = Map.empty[Int, String]
    idpackage.id = 0
    recursivelyRequest(conf, plan, 0, writer, idOpMapOrg)
    // val recursivelyResult = recursivelyRequest(conf, plan, 0, writer, idOpMapOrg)
    // val idOpMap = recursivelyResult._3
    // sej : Datastructure maps id number and Operation name.
    // ex) 2 -> FilterExec
    // logInfo("[DyOp_sej] recursivelyRequest Ends.")
    writer.println("end")
    writer.flush()
    writer.close()
    socket.close()
    // logInfo("[DyOp_sej] recursivelyRequest finally Ends.")

    // receives the result from port 12345
    // ex) GG1 -> 112.128
    // sej : aware of empty Plan set.
    if (plan.childPlans.headOption != None) {
      val estimatedExecMap = receiveEstimatedExecTime()
      // logInfo(s"[DyOp_sej] estimatedExecMap : ${estimatedExecMap}")
      val shortestPath = runShortestPath(estimatedExecMap)
      // shortestPath form (String)
      // 13->G,12->G,11->G,10->G,9->G,8->G,7->G,6->G,5->G,4->G,3->G,2->G,1->C,0->C
      // logInfo(s"[DyOp_sej] shortestPath : ${shortestPath}")
      val shortestPathMap = convertShortestPathToMap(shortestPath)
      logInfo(s"[DyOp_sej] shortestPathMap : ${shortestPathMap}")
      idpackage.id = 0
      dyOpRecursivelyOptimize(conf, plan, 0, shortestPathMap, optimizations)
      // shortestPathMap form (String) : Map(13->True,12->True,...,1->False,0->False)
      // idOpMap : Map(8 -> ProjectExec, 11 -> ProjectExec,...)
      // logInfo(s"[DyOp_sej] idOpMap: ${idOpMap}")
    }
    // sej end
/*
    if (conf.lmOptimizerEnabled) {
      lmRecursivelyOptimize(conf, plan, optimizations, finalOperator = true)
    } else {
      recursivelyOptimize(conf, cpuCostModel, gpuCostModel, plan, optimizations,
        finalOperator = true)
    }
*/
    if (optimizations.isEmpty) {
      logInfo(s"[Rapids_jgh] optimization is empty")
      logTrace(s"CBO finished optimizing plan. No optimizations applied.")
    } else {
      logInfo(s"[Rapids_jgh] optimization is not empty")
      // jgh: logTrace to logInfo
      logInfo(s"CBO finished optimizing plan. " +
        s"${optimizations.length} optimizations applied:\n\t${optimizations.mkString("\n\t")}")
    }
    idpackage.optimizations = optimizations
    optimizations
  }
  /** sej
  * If true, it means map the operation to cpu. else it means map the operation to gpu.
  * form : 13->G,12->G,11->G,10->G,9->G,8->G,7->G,6->G,5->G,4->G,3->G,2->G,1->C,0->C
  * @param shortestPath String that has information about optimized map.
  *
  * @return Map[Int, Boolean], Map that has optimized shortest path.
  */
  private def convertShortestPathToMap(shortestPath : String): Map[Int, Boolean] = {
    // Split the string by comma and arrow (->) to get key-value pairs
    logInfo(s"[DynO shortestPath] ${shortestPath}")
    val keyValuePairs = shortestPath.split(",").map { entry =>
      val Array(key, value) = entry.split("->")
      key.toInt -> (value == "C")
    }
    // val map: Map[Int, Boolean] = Map(keyValuePairs: _*)
    Map(keyValuePairs: _*)
  }
  /** sej
  * returns choosen device with id number. 
  * @param estimatedExecMap Map datastructure about estimated execution time.
  *
  * @return String, final choosen path.
  */
  private def runShortestPath(estimatedExecMap : Map[String, Double]): String = {
    // 0th operator only can use CC or CG 
    // CC : previous CPU - current CPU
    var id = 0
    // In order of sequence, CC / GC / CG / GG
    // Index of these are -> 0  / 1  / 2  / 3
    var pathLength : Array[Double] = Array(Double.PositiveInfinity, Double.PositiveInfinity, 
                                           Double.PositiveInfinity, Double.PositiveInfinity)
    val tempLength : Array[Double] = Array(Double.PositiveInfinity, Double.PositiveInfinity, 
                                           Double.PositiveInfinity, Double.PositiveInfinity)
    var path : Array[String] = Array("", "", "", "")
    val temp : Array[String] = Array("", "", "", "")

    logInfo(s"[DynO Map] estimatedExecMap: ${estimatedExecMap}")
    while(estimatedExecMap.contains("CC"+id)) {
      id += 1
    }
    id -= 1
    val maxId = id

    while(id >= 0) {
        if (!estimatedExecMap.contains("GC"+id)) {
          estimatedExecMap += ("GC"+id -> Double.PositiveInfinity)
          estimatedExecMap += ("GG"+id -> Double.PositiveInfinity)
        }
        if (id == maxId) {
            tempLength(0) = estimatedExecMap("CC"+id)
            temp(0) = id+"->C"
            tempLength(2) = estimatedExecMap("CG"+id)
            temp(2) = id+"->G"
        }   
        else {
            // in CC case, compare CC / GC
            // in GC case, compare CG / GG
            if (pathLength(0) < pathLength(1)) {
                tempLength(0) = pathLength(0) + estimatedExecMap("CC"+id)
                temp(0) = path(0)+","+id+"->C"
                tempLength(2) = pathLength(0) + estimatedExecMap("CG"+id)
                temp(2) = path(0)+","+id+"->G"
            }
            // in CC case, compare CC / GC
            // in GC case, compare CG / GG
            else {
                tempLength(0) = pathLength(1) + estimatedExecMap("CC"+id)
                temp(0) = path(1)+","+id+"->C"
                tempLength(2) = pathLength(1) + estimatedExecMap("CG"+id)
                temp(2) = path(1)+","+id+"->G"
            }
            // in CG case, compare CC / GC
            // in GG case, compare CG / GG
            if (pathLength(2) < pathLength(3)) {
                tempLength(1) = pathLength(2) + estimatedExecMap("GC"+id)
                temp(1) = path(2)+","+id+"->C"
                tempLength(3) = pathLength(2) + estimatedExecMap("GG"+id)
                temp(3) = path(2)+","+id+"->G"
            }
            // in CG case, compare CC / GC
            // in GG case, compare CG / GG
            else {
                tempLength(1) = pathLength(3) + estimatedExecMap("GC"+id)
                temp(1) = path(3)+","+id+"->C"
                tempLength(3) = pathLength(3) + estimatedExecMap("GG"+id)
                temp(3) = path(3)+","+id+"->G"
            }
            /*
            tempLength(0) = math.min(pathLength(0), pathLength(1)) + estimatedExecMap("CC"+id)
            temp(0) += path(0)+","+"CC"+id
            // in GC case, compare CG / GG
            tempLength(1) = math.min(pathLength(2), pathLength(3)) + estimatedExecMap("GC"+id)
            // in CG case, compare CC / GC
            tempLength(2) = math.min(pathLength(0), pathLength(1)) + estimatedExecMap("CG"+id)
            // in GG case, compare CG / GG
            tempLength(3) = math.min(pathLength(2), pathLength(3)) + estimatedExecMap("GG"+id)
            */
        }
        pathLength = tempLength.clone()
        path = temp.clone()
        id -= 1
    }
    val shortestPathLength = pathLength.reduce((x, y) => if (x < y) x else y)
    val shortestPathIndex = pathLength.indexOf(shortestPathLength)

    // println(pathLength)
    // println(s"The minimum value is: ${shortestPathLength}")
    logInfo(s"[DynO shortestPath_in] ${path(shortestPathIndex)}")
    path(shortestPathIndex)
  }
  /** sej 
  * convert python-style dict to Map data structure
  * @param line string type of python-style dictionary
  *
  * @return Map[String, Float] // ex GG1 -> 112.128
  */
  private def convertDictToMap(line: String): Map[String, Double] = {
    val keyValuePairs = line.stripMargin
                            .stripPrefix("{")
                            .stripSuffix("}")
                            .split(",")
                            .map { pair =>
                                val Array(key, value) = pair.trim.stripPrefix("'").split(":")
                                (key.trim.stripSuffix("'"), value.trim.toDouble)
                            }
    val scalaMap = Map(keyValuePairs: _*)
    scalaMap
  }
  /** sej
  * Recevices the result from tfBackbone.py
  *
  * @return Map[String, Float], ex) GG1 -> 112.128
  */
  private def receiveEstimatedExecTime(): Map[String, Double] = {
    val host = "localhost"
    val port = 12345
    var line = "null"
    // logInfo("[DyOp_sej] Enters receviceEstimatedExecTime.")
    do {
      breakable {
        try {
          // logInfo("[DyOp_sej] Try receviceEstimatedExecTime.")
          val socket = new Socket(host, port)
          val inputStream = socket.getInputStream
          val reader = new BufferedReader(new InputStreamReader(inputStream))
          // logInfo("[DyOp_sej] Mids receviceEstimatedExecTime.")
          line = reader.readLine()
          // logInfo("[DyOp_sej] Readers receviceEstimatedExecTime.")
          socket.close()
          // logInfo("[DyOp_sej] Close the socket, receviceEstimatedExecTime.")
          Thread.sleep(100)
          // logInfo("[DyOp_sej] receviceEstimatedExecTime : waiting for connection.")
        } catch {
          case ex : Exception => {
            logInfo(s"[DyOp_sej] ${ex}")
            break
          }
        }
      }
    } while(line == null || line == "null" )
    // logInfo(s"[DyOp_sej] receviceEstimatedExecTime : line value => ${line}")
    val estimatedExecMap = convertDictToMap(line)
    // logInfo(s"[DynO Map] ${estimatedExecMap}")
    estimatedExecMap
  }
  /** sej
  * Request to Student Model executed by tfBackbone.py.
  * @param conf The configure in RAPIDS micro batch.
  * @param plan The plan to walk through.
  * @param id request id for sequential.
  * @param writer writer that writes at request pipe.
  * @param idOpMap Map that id and operator name.
  *
  * @return (Int, String, Map, Int). It write down needed requests to request pipe
  */
  private def recursivelyRequest(
                                  conf: RapidsConf,
                                  plan: SparkPlanMeta[SparkPlan],
                                  id: Int,
                                  writer: PrintWriter,
                                  idOpMap: Map[Int, String]): (Int, String, Map[Int, String]) = {
    
    val statefulOps = List("StateStoreSaveExec", 
                           "StateStoreRestoreExec", 
                           "StreamingSymmetricHashJoinExec")
    // val childOption = plan.childPlans.headOption
    // val currID = idpackage.id
    var currOp = "None"
    var flag = false
    // [TODO] How to get estimatedRow in RAPIDS?
    val rowCount = ModiRowCountPlanVisitor
      .visit(plan)
      .map(_.toInt)
      .getOrElse(0)

    plan.childPlans.reverse.foreach {
      case (child) => 
        val currID = idpackage.id
        currOp = child.wrapped.getClass.getSimpleName
        idOpMap += (currID -> currOp)
        // if (!flag) {
        //  idpackage.id += 1
        // }
        idpackage.id += 1
        val prevInfo = recursivelyRequest(conf, child, id+1, writer, idOpMap)
        // [TODO] write at request pipe through writer
        // Request Information : requestID, inputRow, opName, p_opName, mapDevice, p_mapDeivce
        // Request ID Format : "C/G (mapDevice)" + "C/G (p_mapDeivce)" + id
        if (!flag) {
          val message = currID+","+prevInfo._1+","+currOp+","+prevInfo._2+","
          writer.println("CC"+message+"CPU,CPU")
          writer.println("CG"+message+"CPU,GPU")
          // do not request if the operator is Stateful operator. (cannot run on GPU)
          if (statefulOps.contains(currOp)) {
            writer.flush()
          }
          else {
            writer.println("GC"+message+"GPU,CPU")
            writer.println("GG"+message+"GPU,GPU")
            writer.flush()
          }
          flag = true
        }
        else {
          val message = currID+","+prevInfo._1+","+currOp+","+prevInfo._2+","
          writer.println("CC"+message+"CPU,CPU")
          writer.println("CG"+message+"CPU,GPU")
          // do not request if the operator is Stateful operator. (cannot run on GPU)
          if (statefulOps.contains(currOp)) {
            writer.flush()
          }
          else {
            writer.println("GC"+message+"GPU,CPU")
            writer.println("GG"+message+"GPU,GPU")
            writer.flush()
          }
          return (rowCount, currOp, prevInfo._3)
        }
    }
    return (rowCount, currOp, idOpMap)
    /*
    childOption match {
      case Some(child) => {
        // logInfo(s"[DyOp_sej] Operation Name(${id}) : ${child.wrapped.getClass.getSimpleName}")

        val currOp = child.wrapped.getClass.getSimpleName
        idOpMap += (id -> currOp)
        idpackage.id += 1
        val prevInfo = recursivelyRequest(conf, child, id+1, writer, idOpMap)
        // [TODO] write at request pipe through writer
        // Request Information : requestID, inputRow, opName, p_opName, mapDevice, p_mapDeivce
        // Request ID Format : "C/G (mapDevice)" + "C/G (p_mapDeivce)" + id
        val message = currID+","+prevInfo._1+","+currOp+","+prevInfo._2+","
        writer.println("CC"+message+"CPU,CPU")
        writer.println("CG"+message+"CPU,GPU")
        // do not request if the operator is Stateful operator. (cannot run on GPU)
        if (statefulOps.contains(currOp)) {
          writer.flush()
        }
        else {
          writer.println("GC"+message+"GPU,CPU")
          writer.println("GG"+message+"GPU,GPU")
          writer.flush()
        }
        (rowCount, currOp, prevInfo._3)
      }      
      case None => {
        // logInfo("[DyOp_sej] Seq is empty")
        val none = "None"
        (rowCount, none, idOpMap)
      }
    }
    */
  }
  /** sej
  * Walk the plan and determine CPU and GPU costs for each operator and then make decisions
  * about whether operators should run on CPU or GPU. (DyOp version)
  *
  * @param conf configuratrion in RAPIDS
  * @param plan The plan to optimize
  * @param id Id node of shortestPathMap
  * @param shortestPathMap Map value of optimized shortestPath
  * @param optimizations Accumulator to store the optimizations that are applied
  *
  * @return Unit
  */
  private def dyOpRecursivelyOptimize(
                                   conf: RapidsConf,
                                   plan: SparkPlanMeta[SparkPlan],
                                   id: Int,
                                   shortestPathMap: Map[Int, Boolean],
                                   optimizations: ListBuffer[Optimization]): Unit = {

    // val childOption = plan.childPlans.headOption
    plan.childPlans.reverse.foreach {
      case (child) => {
        val currID = idpackage.id
        idpackage.id += 1
        dyOpRecursivelyOptimize(conf, child, id+1, shortestPathMap, optimizations)
        // if this operation are mapped in CPU
        if (shortestPathMap(currID)) {
          // keep this operation on CPU      
          optimizations.append(ModiAvoidTransition(plan))
          plan.costPreventsRunningOnGpu()
        }
      }
    }
  }
  /**
  * Walk the plan and determine CPU and GPU costs for each operator and then make decisions
  * about whether operators should run on CPU or GPU.
  *
  * @param plan The plan to optimize
  * @param optimizations Accumulator to store the optimizations that are applied
  * @param finalOperator Is this the final (root) operator? We have special behavior for this
  *                      case because we need the final output to be on the CPU in row format
  * @return Tuple containing (cpuCost, gpuCost) for the specified plan and the subset of the
  *         tree beneath it that is a candidate for optimization.
  */
  private def recursivelyOptimize(
                                   conf: RapidsConf,
                                   cpuCostModel: ModiCostModel,
                                   gpuCostModel: ModiCostModel,
                                   plan: SparkPlanMeta[SparkPlan],
                                   optimizations: ListBuffer[Optimization],
                                   finalOperator: Boolean): (Double, Double) = {

    val childplan: Seq[String] = plan.childPlans.map(_.wrapped.getClass.getSimpleName) // jgh
    logInfo(s"[Rapids_jgh] recursiveOp wrap's name : ${plan.wrapped.getClass.getSimpleName} \n" +
      s"recursiveOp wrap's parent : ${plan.parent.map(_.wrapped.getClass.getSimpleName)} \n" +
      s"recursiveOp wrap's childPlans : ${childplan} \n" +
      s"recursiveOp wrap's num of childPlans : ${plan.childPlans.size} \n" +
      s"recursiveOp wrap's Class Type : ${plan.getClass.getSimpleName}") // jgh

    // get the CPU and GPU cost of this operator (excluding cost of children)
    val operatorCpuCost = cpuCostModel.getCost(plan)
    val operatorGpuCost = gpuCostModel.getCost(plan)

    // get the CPU and GPU cost of the child plan(s)
    val childCosts = plan.childPlans
      .map(child => recursivelyOptimize(
        conf,
        cpuCostModel,
        gpuCostModel,
        child,
        optimizations,
        finalOperator = false))
    val (childCpuCosts, childGpuCosts) = childCosts.unzip

    // calculate total (this operator + children)
    val totalCpuCost = operatorCpuCost + childCpuCosts.sum
    var totalGpuCost = operatorGpuCost + childGpuCosts.sum
    logCosts(plan, "Operator costs", operatorCpuCost, operatorGpuCost)
    logCosts(plan, "Operator + child costs", totalCpuCost, totalGpuCost)

    plan.estimatedOutputRows = ModiRowCountPlanVisitor.visit(plan)
    /*
    plan.estimatedOutputRows match {
          case Some(value) => {
            logInfo(s"[DyOp_sej] recursiveOptimize value is ${value}")
          }
          case None => {
            logInfo(s"[DyOp_sej] recursiveOptimize value is ${plan.estimatedOutputRows}")
          }
    }
    */


    // determine how many transitions between CPU and GPU are taking place between
    // the child operators and this operator
    val numTransitions = plan.childPlans
      .count(canRunOnGpu(_) != canRunOnGpu(plan))
    logCosts(plan, s"numTransitions=$numTransitions", totalCpuCost, totalGpuCost)

    if (numTransitions > 0) {
      // there are transitions between CPU and GPU so we need to calculate the transition costs
      // and also make decisions based on those costs to see whether any parts of the plan would
      // have been better off just staying on the CPU

      // is this operator on the GPU?
      if (canRunOnGpu(plan)) {
        // at least one child is transitioning from CPU to GPU so we calculate the
        // transition costs
        val transitionCost = plan.childPlans
          .filterNot(canRunOnGpu)
          .map(transitionToGpuCost(conf, _)).sum

        // if the GPU cost including transition is more than the CPU cost then avoid this
        // transition and reset the GPU cost
        if (operatorGpuCost + transitionCost > operatorCpuCost && !isExchangeOp(plan)) {
          // avoid transition and keep this operator on CPU
          optimizations.append(ModiAvoidTransition(plan))
          plan.costPreventsRunningOnGpu()
          // reset GPU cost
          totalGpuCost = totalCpuCost
          logCosts(plan, s"Avoid transition to GPU", totalCpuCost, totalGpuCost)
        } else {
          // add transition cost to total GPU cost
          totalGpuCost += transitionCost
          logCosts(plan, s"transitionFromCpuCost=$transitionCost",
            totalCpuCost, totalGpuCost)
        }
      } else {
        // at least one child is transitioning from GPU to CPU so we evaluate each of this
        // child plans to see if it was worth running on GPU now that we have the cost of
        // transitioning back to CPU
        plan.childPlans.zip(childCosts).foreach {
          case (child, childCosts) =>
            val (childCpuCost, childGpuCost) = childCosts
            val transitionCost = transitionToCpuCost(conf, child)
            val childGpuTotal = childGpuCost + transitionCost
            if (canRunOnGpu(child) && !isExchangeOp(child)
              && childGpuTotal > childCpuCost) {
              // force this child plan back onto CPU
              optimizations.append(ModiReplaceSection(
                child, totalCpuCost, totalGpuCost))
              child.recursiveCostPreventsRunningOnGpu()
            }
        }

        // recalculate the transition costs because child plans may have changed
        val transitionCost = plan.childPlans
          .filter(canRunOnGpu)
          .map(transitionToCpuCost(conf, _)).sum
        totalGpuCost += transitionCost
        logCosts(plan, s"transitionFromGpuCost=$transitionCost",
          totalCpuCost, totalGpuCost)
      }
    }

    // special behavior if this is the final operator in the plan because we always have the
    // cost of going back to CPU at the end
    if (finalOperator && canRunOnGpu(plan)) {
      val transitionCost = transitionToCpuCost(conf, plan)
      totalGpuCost += transitionCost
      logCosts(plan, s"final operator, transitionFromGpuCost=$transitionCost",
        totalCpuCost, totalGpuCost)
    }

    if (totalGpuCost > totalCpuCost) {
      // we have reached a point where we have transitioned onto GPU for part of this
      // plan but with no benefit from doing so, so we want to undo this and go back to CPU
      if (canRunOnGpu(plan) && !isExchangeOp(plan)) {
        // this plan would have been on GPU so we move it and onto CPU and recurse down
        // until we reach a part of the plan that is already on CPU and then stop
        optimizations.append(ModiReplaceSection(plan, totalCpuCost, totalGpuCost))
        plan.recursiveCostPreventsRunningOnGpu()
        // reset the costs because this section of the plan was not moved to GPU
        totalGpuCost = totalCpuCost
        logCosts(plan, s"ModiReplaceSection: ${plan}", totalCpuCost, totalGpuCost)
      }
    }

    if (!canRunOnGpu(plan) || isExchangeOp(plan)) {
      // reset the costs because this section of the plan was not moved to GPU
      totalGpuCost = totalCpuCost
      logCosts(plan, s"Reset costs (not on GPU / exchange)", totalCpuCost, totalGpuCost)
    }

    logCosts(plan, "END", totalCpuCost, totalGpuCost)
    (totalCpuCost, totalGpuCost)
  }

  /**
   * Walk the plan and determine CPU and GPU costs for each operator and then make decisions
   * about whether operators should run on CPU or GPU.
   *
   * @param plan The plan to optimize
   * @param optimizations Accumulator to store the optimizations that are applied
   * @param finalOperator Is this the final (root) operator? We have special behavior for this
   *                      case because we need the final output to be on the CPU in row format
   * @return Tuple containing (cpuCost, gpuCost) for the specified plan and the subset of the
   *         tree beneath it that is a candidate for optimization.
   */
  private def lmRecursivelyOptimize(
    conf: RapidsConf,
    plan: SparkPlanMeta[SparkPlan],
    optimizations: ListBuffer[Optimization],
    finalOperator: Boolean): (Double, Double) = {

    // get the CPU and GPU cost of the child plan(s)
    val childCosts = plan.childPlans
      .map(child => lmRecursivelyOptimize(
        conf,
        child.asInstanceOf[SparkPlanMeta[SparkPlan]],
        optimizations,
        finalOperator = false))

    // logInfo("----------------------------------------------------------")
    // logInfo(s"[RAPIDS] Current operation: ${plan.wrapped.getClass.getSimpleName}")
    // logInfo(s"[RAPIDS] Is this operator on the GPU? (initial): ${plan.canThisBeReplaced}")

    /* plan.parent match {
        case Some(p) =>
            logInfo(s"[RAPIDS] parent: ${p.wrapped.getClass.getSimpleName}")
        case None =>
            logInfo("[RAPIDS] Root of plan")
    } */

    val (childCpuCosts, childGpuCosts) = childCosts.unzip

    // get the CPU and GPU cost of this operator (excluding cost of children)
    val (operatorCpuCost, operatorGpuCost) = costModel.applyCost(plan,
      currentBatchSize, inflectionPointOfBatchSize)

    // calculate total (this operator + children)
    val totalCpuCost = operatorCpuCost + childCpuCosts.sum
    var totalGpuCost = operatorGpuCost + childGpuCosts.sum

    /* logInfo(s"[RAPIDS] childCpuCosts: $childCpuCosts")
    logInfo(s"[RAPIDS] childGpuCosts: $childGpuCosts")
    logInfo(s"[RAPIDS] NormalizedBatchSize: ${(currentBatchSize / inflectionPointOfBatchSize)}")
    logInfo(s"[Rapids] operatorCpuCost: $operatorCpuCost")
    logInfo(s"[Rapids] operatorGpuCost: $operatorGpuCost")
    logInfo(s"[Rapids] totalCpuCost (before): $totalCpuCost")
    logInfo(s"[Rapids] totalGpuCost (before): $totalGpuCost") */

    plan.estimatedOutputRows = ModiRowCountPlanVisitor.visit(plan)

    // determine how many transitions between CPU and GPU are taking place between
    // the child operators and this operator
    val numTransitions = plan.childPlans
      .count(_.canThisBeReplaced != plan.canThisBeReplaced)

    if (numTransitions > 0) {
      // there are transitions between CPU and GPU so we need to calculate the transition costs
      // and also make decisions based on those costs to see whether any parts of the plan would
      // have been better off just staying on the CPU

      // is this operator on the GPU?
      if (plan.canThisBeReplaced) {
        // logInfo("[RAPIDS] This operator is on the GPU (numTransition > 0)")
        // at least one child is transitioning from CPU to GPU so we calculate the
        // transition costs
        val transitionCost = plan.childPlans.filter(!_.canThisBeReplaced)
          .map(lmTransitionToGpuCost(conf, _, currentBatchSize, inflectionPointOfBatchSize)).sum

        // if the GPU cost including transition is more than the CPU cost then avoid this
        // transition and reset the GPU cost
        if (operatorGpuCost + transitionCost > operatorCpuCost && !consumesQueryStage(plan)) {
          // avoid transition and keep this operator on CPU
          println(s"[RAPIDS] Operation ${plan.wrapped.getClass.getSimpleName}" +
            s"moved to CPU by cost model.")
          optimizations.append(ModiAvoidTransition(plan))
          plan.costPreventsRunningOnGpu()
          // reset GPU cost
          totalGpuCost = totalCpuCost;
        } else {
          // add transition cost to total GPU cost
          totalGpuCost += transitionCost
        }
      } else {
        // logInfo("[RAPIDS] This operator is not on the GPU (numTransition > 0)")
        // at least one child is transitioning from GPU to CPU so we evaulate each of this
        // child plans to see if it was worth running on GPU now that we have the cost of
        // transitioning back to CPU
        plan.childPlans.zip(childCosts).foreach {
          case (child, childCosts) =>
            val (childCpuCost, childGpuCost) = childCosts
            val transitionCost = lmTransitionToCpuCost(conf, child,
              currentBatchSize, inflectionPointOfBatchSize)
            val childGpuTotal = childGpuCost + transitionCost
            if (child.canThisBeReplaced && !consumesQueryStage(child)
              && childGpuTotal > childCpuCost) {
              println(s"[RAPIDS] Child operations of ${plan.wrapped.getClass.getSimpleName}" +
                s"moved to CPU by original cost model.")
              // force this child plan back onto CPU
              optimizations.append(ModiReplaceSection(
                child.asInstanceOf[SparkPlanMeta[SparkPlan]], totalCpuCost, totalGpuCost))
              child.recursiveCostPreventsRunningOnGpu()
            }
        }

        // recalculate the transition costs because child plans may have changed
        val transitionCost = plan.childPlans
          .filter(_.canThisBeReplaced)
          .map(lmTransitionToCpuCost(conf, _,
            currentBatchSize, inflectionPointOfBatchSize)).sum
        totalGpuCost += transitionCost
      }
    }

    // special behavior if this is the final operator in the plan because we always have the
    // cost of going back to CPU at the end
    if (finalOperator && plan.canThisBeReplaced) {
      // logInfo("[RAPIDS] Final operator behavior: Add cost of going back to the CPU.")
      totalGpuCost += lmTransitionToCpuCost(conf, plan,
        currentBatchSize, inflectionPointOfBatchSize)
    }

    // logInfo(s"[Rapids] totalCpuCost (last): $totalCpuCost")
    // logInfo(s"[Rapids] totalGpuCost (last): $totalGpuCost")

    if (totalGpuCost > totalCpuCost) {
      // println("[RAPIDS] totalGpuCost > totalCpuCost")
      // we have reached a point where we have transitioned onto GPU for part of this
      // plan but with no benefit from doing so, so we want to undo this and go back to CPU
      if (plan.canThisBeReplaced && !consumesQueryStage(plan)) {
        // this plan would have been on GPU so we move it and onto CPU and recurse down
        // until we reach a part of the plan that is already on CPU and then stop
        optimizations.append(ModiReplaceSection(plan, totalCpuCost, totalGpuCost))
        plan.recursiveCostPreventsRunningOnGpu()
        println(s"[RAPIDS] Operation ${plan.wrapped.getClass.getSimpleName}" +
          s"moved to CPU by cost model.")
        // reset the costs because this section of the plan was not moved to GPU
        totalGpuCost = totalCpuCost
      }
    }

    // Custom special behavior (1): Stream + Batch jobs.
    // Inform that this is batch source read & Set its size as current batch size.
    // If BroadcastHashJoinExec will be on the CPU, also put BroadcastExchangeExec on the CPU.
    // If BroadcastHashJoinExec will be on the GPU, also put BroadcastExchangeExec on the GPU.
    plan match {
      case p =>
        if (sizeOfBatchedFileSource != -1.0 &&
          p.wrapped.getClass.getSimpleName == "BroadcastExchangeExec") {
          p.parent match {
            case Some(pp) =>
              if (pp.wrapped.getClass.getSimpleName == "BroadcastHashJoinExec") {
                println("[RAPIDS] Start processing batch data source.")
                // currentBatchSize = sizeOfBatchedFileSource
                currentBatchSize = sizeOfBatchedFileSource / numPartitions
                val cudaMemInfo = Cuda.memGetInfo()
                //  Should we use free memory or need to consider allocation fraction?
                val gpuMemFree = cudaMemInfo.free
                // If currentBatchSize is too large, BroadcastHashJoinExec will be moved to CPU;
                // which means BroadcastExchangeExec must be on the CPU too.
                //  Get estimated input size of BroadcastHashJoinExec using function.
                if (currentBatchSize > (gpuMemFree * numTotalGpuDevices)) {
                  if (plan.canThisBeReplaced && !consumesQueryStage(plan)) {
                    // avoid transition and keep this operator on CPU
                    optimizations.append(ModiAvoidTransition(plan))
                    plan.costPreventsRunningOnGpu()
                    println(s"[RAPIDS] Operation ${plan.wrapped.getClass.getSimpleName}" +
                      s"moved to CPU to prevent GPU out-of-memory error.")
                    // reset GPU cost
                    totalGpuCost = totalCpuCost;
                  }
                }
              }
            case None =>
              logInfo("[RAPIDS] Root of plan")
          }
        }
    }

    // Custom special behavior (2): The broadcast must be on the same device.
    // If BroadcastExchangeExec moved to the CPU, force BraodcastHashJoinExec on the CPU.
    if (plan.wrapped.getClass.getSimpleName == "BroadcastHashJoinExec" &&
      plan.childPlans.head.wrapped.getClass.getSimpleName == "BroadcastExchangeExec" &&
      !plan.childPlans.head.canThisBeReplaced) {
      if (plan.canThisBeReplaced && !consumesQueryStage(plan)) {
        // avoid transition and keep this operator on CPU
        optimizations.append(ModiAvoidTransition(plan))
        plan.costPreventsRunningOnGpu()
        println(s"[RAPIDS] Both ${plan.wrapped.getClass.getSimpleName} and" +
          s"${plan.childPlans.head.wrapped.getClass.getSimpleName} run on the CPU.")
        // reset GPU cost
        totalGpuCost = totalCpuCost;
      }
    }

    // Custom special behavior (3): When there is running out of GPU device memory.
    // If it is the first operator (scan) or
    // current operator's child operator has been moved to the CPU,
    // and current operator is on the GPU,
    // then we need to check data size and GPU free memory to prevent GPU out-of-memory error.
    if ((plan.childPlans.size == 0 || !plan.childPlans.head.canThisBeReplaced) &&
      plan.canThisBeReplaced) {
      val maxInputSize = lmEstimateInputSize(conf, plan,
        currentBatchSize, previousOutputSize)
      val cudaMemInfo = Cuda.memGetInfo()
      //  Should we use free memory or need to consider allocation fraction?
      val gpuMemFree = cudaMemInfo.free
      // logInfo(s"[Rapids] maxInputSize: $maxInputSize")
      // logInfo(s"[Rapids] gpuDeviceNum: $numTotalGpuDevices")
      // logInfo(s"[Rapids] GPU free memory per 1 device: $gpuMemFree")

      if (maxInputSize > (gpuMemFree * numTotalGpuDevices)) {
        // If estmiated maximum size of input requires more memory than available GPU memory,
        // we should undo this and go back to CPU.
        // println("[Rapids] maxInputSize > gpuMemFree")
        if (plan.canThisBeReplaced && !consumesQueryStage(plan)) {
          // avoid transition and keep this operator on CPU
          optimizations.append(ModiAvoidTransition(plan))
          plan.costPreventsRunningOnGpu()
          println(s"[RAPIDS] Operation ${plan.wrapped.getClass.getSimpleName}" +
            s"moved to CPU to prevent GPU out-of-memory error.")
          // reset GPU cost
          totalGpuCost = totalCpuCost;
        }
      }
    }

    // Save max estimation of current opertor's output size
    // to use for estimation of next operator's input size.
    previousOutputSize = lmEstimateOutputSize(conf, plan,
      currentBatchSize, inflectionPointOfBatchSize)

    if (!plan.canThisBeReplaced || consumesQueryStage(plan)) {
      // reset the costs because this section of the plan was not moved to GPU
      totalGpuCost = totalCpuCost
    }

    // logInfo(s"[Rapids] totalCpuCost (final): $totalCpuCost")
    // logInfo(s"[Rapids] totalGpuCost (final): $totalGpuCost")
    // logInfo(s"[RAPIDS] Is this operator on the GPU? (final): ${plan.canThisBeReplaced}")

    (totalCpuCost, totalGpuCost)
  }

  private def logCosts(
                        plan: SparkPlanMeta[_],
                        message: String,
                        cpuCost: Double,
                        gpuCost: Double): Unit = {
    val sign = if (cpuCost == gpuCost) {
      "=="
    } else if (cpuCost < gpuCost) {
      "<"
    } else {
      ">"
    }
    logTrace(s"CBO [${plan.wrapped.getClass.getSimpleName}] $message: " +
      s"cpuCost=$cpuCost $sign gpuCost=$gpuCost)")
  }

  private def canRunOnGpu(plan: SparkPlanMeta[_]): Boolean = plan.wrapped match {
    case _: AdaptiveSparkPlanExec =>
      // this is hacky but AdaptiveSparkPlanExec is always tagged as "cannot replace" and
      // there are no child plans to inspect, so we just assume that the plan is running
      // on GPU
      true
    case _ => plan.canThisBeReplaced
  }

  private def transitionToGpuCost(conf: RapidsConf, plan: SparkPlanMeta[SparkPlan]): Double = {
    val rowCount = ModiRowCountPlanVisitor.visit(plan).map(_.toDouble)
      .getOrElse(conf.defaultRowCount.toDouble)
    val dataSize = ModiMemoryCostHelper.estimateGpuMemory(plan.wrapped.schema, rowCount)
    conf.getGpuOperatorCost("GpuRowToColumnarExec").getOrElse(0d) * rowCount +
      ModiMemoryCostHelper.calculateCost(dataSize, conf.cpuReadMemorySpeed) +
      ModiMemoryCostHelper.calculateCost(dataSize, conf.gpuWriteMemorySpeed)
  }

  private def transitionToCpuCost(conf: RapidsConf, plan: SparkPlanMeta[SparkPlan]): Double = {
    val rowCount = ModiRowCountPlanVisitor.visit(plan).map(_.toDouble)
      .getOrElse(conf.defaultRowCount.toDouble)
    val dataSize = ModiMemoryCostHelper.estimateGpuMemory(plan.wrapped.schema, rowCount)
    conf.getGpuOperatorCost("GpuColumnarToRowExec").getOrElse(0d) * rowCount +
      ModiMemoryCostHelper.calculateCost(dataSize, conf.gpuReadMemorySpeed) +
      ModiMemoryCostHelper.calculateCost(dataSize, conf.cpuWriteMemorySpeed)
  }

  // jgh: for LMStream
  private def lmTransitionToGpuCost(conf: RapidsConf,
                                  plan: SparkPlanMeta[_],
                                  currentBatchSize: Double,
                                  inflectionPointOfBatchSize: Double) = {
    // Original: this is a placeholder for now;
    // we would want to try and calculate the transition cost
    // based on the data types and size (if known)
    // conf.defaultTransitionToGpuCost // 0.1

    // We should normalize score by reflecting batch size.
    // If current batch size is large -> transitionToGpuCost gets bigger -> avoid GPUs

    // jgh: The defaultTransitionToGpuCost conf does not exist in the 23.06.0 version,
    // so set it to 0.1
//    conf.defaultTransitionToGpuCost * (currentBatchSize / inflectionPointOfBatchSize)
    0.1 * (currentBatchSize / inflectionPointOfBatchSize)
  }

  // jgh: for LMStream
  private def lmTransitionToCpuCost(conf: RapidsConf,
                                  plan: SparkPlanMeta[_],
                                  currentBatchSize: Double,
                                  inflectionPointOfBatchSize: Double) = {
    // this is a placeholder for now - we would want to try and calculate the transition cost
    // based on the data types and size (if known)
    // conf.defaultTransitionToCpuCost // 0.1

    // We should normalize score by reflecting batch size.
    // If current batch size is large -> transitionToCpuCost gets bigger -> avoid CPUs

    // jgh: The defaultTransitionToCpuCost conf does not exist in the 23.06.0 version,
    // so set it to 0.1
//    conf.defaultTransitionToCpuCost * (currentBatchSize / inflectionPointOfBatchSize)
    0.1 * (currentBatchSize / inflectionPointOfBatchSize)
  }

  // jgh: for LMStream
  /**
   * Determines whether the specified operator will read from a query stage.
   */
  private def consumesQueryStage(plan: SparkPlanMeta[_]): Boolean = {
    // if the child query stage already executed on GPU then we need to keep the
    // next operator on GPU in these cases

    // jgh: spark 3.2.3 doen't manage CustomShuffleReaderExec. so delete CustomShuffleReaderExec
    SQLConf.get.adaptiveExecutionEnabled && (plan.wrapped match {
//      case _: CustomShuffleReaderExec
      case _: ShuffledHashJoinExec
           | _: BroadcastHashJoinExec
           | _: BroadcastNestedLoopJoinExec => true
      case _ => false
    })
  }

  // jgh: for LMStream
  private def lmEstimateInputSize(conf: RapidsConf,
                                plan: SparkPlanMeta[_],
                                currentBatchSize: Double,
                                previousOutputSize: Double) = {
    // Return max estimation of input size for each operators;
    // If operator is scan, input size is same as currentBatchSize
    // else if for other operators, input size is same as output size of previous operators
    plan.wrapped match {
      case _: FileSourceScanExec =>
        currentBatchSize
      case _ =>
        previousOutputSize
    }
  }

  // jgh: for LMStream
  private def lmEstimateOutputSize(conf: RapidsConf,
                                 plan: SparkPlanMeta[_],
                                 currentBatchSize: Double,
                                 inflectionPointOfBatchSize: Double) = {
    // Return max estimation of output size for each operators;
    // stateless operators' max output size is same as input size
    // stateful operators' max output size is bounded by multiple of input size
    // windowing operators and other streaming-state operators (e.g., stateStoreSaveExec)
    // are fixed on CPU, since it store its incremental state infos in host memory;
    // which results too much transition cost.
    plan.wrapped match {
      case _: ExpandExec => // Stateless operation (Unary)
        currentBatchSize
      case _: FileSourceScanExec => // Stateless operation
        currentBatchSize
      case _: FilterExec => // Stateless operation
        currentBatchSize
      case _: ProjectExec => // Stateless operation
        currentBatchSize
      case _: BroadcastHashJoinExec => // Stateful operation
        currentBatchSize * 2
      case _: HashAggregateExec => // Stateful operation
        currentBatchSize * 2
      case _: ShuffleExchangeExec => // Stateful operation
        currentBatchSize * 2
      case _: SortExec => // Stateful operation
        currentBatchSize * 2
      case _ =>
        currentBatchSize
    }
  }

  /**
   * Determines whether the specified operator is an exchange, or will read from an
   * exchange / query stage. CBO needs to avoid moving these operators back onto
   * CPU because it could result in an invalid plan.
   */
  private def isExchangeOp(plan: SparkPlanMeta[_]): Boolean = {
    // if the child query stage already executed on GPU then we need to keep the
    // next operator on GPU in these cases
    SparkShimImpl.isExchangeOp(plan)
  }
}

/**
 * The cost model is behind a trait so that we can consider making this pluggable in the future
 * so that users can override the cost model to suit specific use cases.
 */
trait ModiCostModel {

  /**
   * Determine the CPU and GPU cost for an individual operator.
   * @param plan Operator
   * @return (cpuCost, gpuCost)
   */
  def getCost(plan: SparkPlanMeta[_]): Double

}

// jgh: for lmStream
/**
 * The cost model is behind a trait so that we can consider making this pluggable in the future
 * so that users can override the cost model to suit specific use cases.
 */
trait LMCostModel {

  /**
   * Determine the CPU and GPU cost for an individual operator.
   * @param plan Operator
   * @return (cpuCost, gpuCost)
   */
  def applyCost(plan: SparkPlanMeta[_],
                currentBatchSize: Double,
                inflectionPointOfBatchSize: Double): (Double, Double)

  /**
   * Determine the cost of transitioning data from CPU to GPU for a specific operator
   * @param plan Operator
   * @return Cost
   */
  /* def transitionToGpuCost(plan: SparkPlanMeta[_],
    currentBatchSize: Double,
    inflectionPointOfBatchSize: Double): Double */

  /**
   * Determine the cost of transitioning data from GPU to CPU for a specific operator
   */
  /* def transitionToCpuCost(plan: SparkPlanMeta[_],
    currentBatchSize: Double,
    inflectionPointOfBatchSize: Double): Double */
}

// jgh : Originally there was no with Logging
class ModiCpuCostModel(conf: RapidsConf) extends ModiCostModel with Logging{

  def getCost(plan: SparkPlanMeta[_]): Double = {

    val rowCount = ModiRowCountPlanVisitor.visit(plan).map(_.toDouble)
      .getOrElse(conf.defaultRowCount.toDouble)

    val operatorCost = plan.conf
      .getCpuOperatorCost(plan.wrapped.getClass.getSimpleName)
      .getOrElse(conf.defaultCpuOperatorCost) * rowCount

    val operatorName = plan.wrapped.getClass.getSimpleName
    val cost = plan.conf
      .getCpuOperatorCost(plan.wrapped.getClass.getSimpleName)
      .getOrElse(conf.defaultCpuOperatorCost)

    val exprEvalCost = plan.childExprs
      .map(expr => exprCost(expr.asInstanceOf[BaseExprMeta[Expression]], rowCount))
      .sum

    logInfo(s"[Rapids_jgh] CPUoperatorName(cost) * rowNum : $operatorName" + s"($cost)" +
      s"* $rowCount")
    logInfo(s"[Rapids_jgh] Total CPU cost jgh$operatorName = operatorCost + exprEvalCost : " +
      s"${operatorCost+exprEvalCost} = $operatorCost + $exprEvalCost")

    operatorCost + exprEvalCost
  }

  private def exprCost[INPUT <: Expression](expr: BaseExprMeta[INPUT], rowCount: Double): Double = {
    if (ModiMemoryCostHelper.isExcludedFromCost(expr)) {
      return 0d
    }

    val memoryReadCost = expr.wrapped match {
      case _: Alias =>
        // alias has no cost, we just evaluate the cost of the aliased expression
        exprCost(expr.childExprs.head.asInstanceOf[BaseExprMeta[Expression]], rowCount)

      case _: AttributeReference | _: GetStructField =>
        ModiMemoryCostHelper.calculateCost(ModiMemoryCostHelper.estimateGpuMemory(
          expr.typeMeta.dataType, nullable = false, rowCount), conf.cpuReadMemorySpeed)

      case _ =>
        expr.childExprs
          .map(e => exprCost(e.asInstanceOf[BaseExprMeta[Expression]], rowCount)).sum
    }

    // the output of evaluating the expression needs to be written out to rows
    val memoryWriteCost = ModiMemoryCostHelper.calculateCost(ModiMemoryCostHelper.estimateGpuMemory(
      expr.typeMeta.dataType, nullable = false, rowCount), conf.cpuWriteMemorySpeed)

    // optional additional per-row overhead of evaluating the expression
    val exprEvalCost = rowCount *
      expr.conf.getCpuExpressionCost(expr.getClass.getSimpleName)
        .getOrElse(conf.defaultCpuExpressionCost)

    exprEvalCost + memoryReadCost + memoryWriteCost
  }
}

class ModiGpuCostModel(conf: RapidsConf) extends ModiCostModel with Logging{

  def getCost(plan: SparkPlanMeta[_]): Double = {
    val rowCount = ModiRowCountPlanVisitor.visit(plan).map(_.toDouble)
      .getOrElse(conf.defaultRowCount.toDouble)

    val operatorCost = plan.conf
      .getGpuOperatorCost(plan.wrapped.getClass.getSimpleName)
      .getOrElse(conf.defaultGpuOperatorCost) * rowCount

    val exprEvalCost = plan.childExprs
      .map(expr => exprCost(expr.asInstanceOf[BaseExprMeta[Expression]], rowCount))
      .sum

    // jgh start
    val operatorName = plan.wrapped.getClass.getSimpleName
    logInfo(s"[Rapids_jgh] GPUoperatorName(cost) * rowNum : $operatorName" + s"($operatorCost)" +
      s"* $rowCount")
    logInfo(s"[Rapids_jgh] Total GPU cost jgh$operatorName = operatorCost + exprEvalCost : " +
      s"${operatorCost+exprEvalCost} = $operatorCost + $exprEvalCost")
    // jgh end

    operatorCost + exprEvalCost
  }

  private def exprCost[INPUT <: Expression](expr: BaseExprMeta[INPUT], rowCount: Double): Double = {
    if (ModiMemoryCostHelper.isExcludedFromCost(expr)) {
      return 0d
    }

    var memoryReadCost = 0d
    var memoryWriteCost = 0d

    expr.wrapped match {
      case _: Alias =>
        // alias has no cost, we just evaluate the cost of the aliased expression
        exprCost(expr.childExprs.head.asInstanceOf[BaseExprMeta[Expression]], rowCount)

      case _: AttributeReference =>
      // referencing an existing column on GPU is almost free since we're
      // just increasing a reference count and not actually copying any data

      case _ =>
        memoryReadCost = expr.childExprs
          .map(e => exprCost(e.asInstanceOf[BaseExprMeta[Expression]], rowCount)).sum

        memoryWriteCost += ModiMemoryCostHelper
          .calculateCost(ModiMemoryCostHelper.estimateGpuMemory(
            expr.typeMeta.dataType, nullable = false, rowCount), conf.gpuWriteMemorySpeed)
    }

    // optional additional per-row overhead of evaluating the expression
    val exprEvalCost = rowCount *
      expr.conf.getGpuExpressionCost(expr.getClass.getSimpleName)
        .getOrElse(conf.defaultGpuExpressionCost)

    exprEvalCost + memoryReadCost + memoryWriteCost
  }
}

// jgh: for LMStream
class DefaultCostModel(conf: RapidsConf) extends LMCostModel {

  /* def transitionToGpuCost(plan: SparkPlanMeta[_]) = {
    // this is a placeholder for now - we would want to try and calculate the transition cost
    // based on the data types and size (if known)
    conf.defaultTransitionToGpuCost
  } */

  /* def transitionToCpuCost(plan: SparkPlanMeta[_]) = {
    // this is a placeholder for now - we would want to try and calculate the transition cost
    // based on the data types and size (if known)
    conf.defaultTransitionToCpuCost
  } */

  override def applyCost(plan: SparkPlanMeta[_],
                         currentBatchSize: Double,
                         inflectionPointOfBatchSize: Double): (Double, Double) = {

    // This is used for normalizing scores by reflecting
    // current batch size and optimized inflection point.
    val normalizedBatchSize = currentBatchSize / inflectionPointOfBatchSize

    // for now we have a constant cost for CPU operations and we make the GPU cost relative
    // to this but later we may want to calculate actual CPU costs
    val cpuCost = 1.0
    // CPU cost is proportional to normalized batch size;
    // which means if current batch size is large -> normalized batch size is large ->
    // then CPU preference goes down -> so we shold make normalizedCpuCost bigger
    // to maintain GPU functions.
    val normalizedCpuCost = cpuCost * normalizedBatchSize

    // always check for user overrides first
    // jgh: In 23.06.0 version, there is no def getOperatorCost. so it change to 0.8.
    // (default GpuOperatorCost is 0.8 in Rapids 0.5 version)
    val gpuCost = plan.conf.getGpuOperatorCost(plan.wrapped.getClass.getSimpleName).getOrElse {
      plan.wrapped match {
        case _: HashAggregateExec =>
          // Prefers CPU
          1.0
        case _: FilterExec =>
          // Prefers CPU
          1.0
        case _: ProjectExec =>
          // Original code: the cost of a projection is the average cost of its expressions
          /* plan.childExprs
              .map(expr => exprCost(expr.asInstanceOf[BaseExprMeta[Expression]]))
              .sum / plan.childExprs.length */
          // Preference score is borderline of CPU and GPU
          0.9
        case _: ShuffleExchangeExec =>
          // setting the GPU cost of ShuffleExchangeExec to 1.0 avoids moving from CPU to GPU for
          // a shuffle. This must happen before the join consistency or we risk running into issues
          // with disabling one exchange that would make a join inconsistent
          // Addition: SuffleExchangeExec prefers CPU since it needs to flush intermediate data
          // from host memory to disk.
          1.0
        case _: BroadcastHashJoinExec =>
          // Preference score is borderline of CPU and GPU
          0.9
        case _: ExpandExec =>
          // Preference score is borderline of CPU and GPU
          0.9
        case _: SortExec =>
          // Prefers GPU
          0.8
        case _: FileSourceScanExec =>
          // Prefers GPU
          0.8
        case _ =>
          // Original code; 0.8
          // conf.defaultOperatorCost
          // For other operators, basically set to prefer GPU
          0.8
      }
    }
    // GPU cost is unproportional to normalized batch size;
    // which means if current batch size is large -> normalized batch size is large ->
    // then GPU preference goes up -> so we should make normalizedGpuCost smaller
    // to not to be replaced by CPU functions.
    val normalizedGpuCost = gpuCost * (1 / normalizedBatchSize)

    // plan.cpuCost = cpuCost
    // plan.gpuCost = gpuCost
    plan.cpuCost = normalizedCpuCost
    plan.gpuCost = normalizedGpuCost

    (normalizedCpuCost, normalizedGpuCost)
  }

//  private def exprCost[INPUT <: Expression](expr: BaseExprMeta[INPUT]): Double = {
//    // always check for user overrides first
//
//    // jgh: getExpressionCost doesn't exist in 23.06.0 version.
//    // change getExpressionCost to getGpuExpressionCost
//    expr.conf.getGpuExpressionCost(expr.getClass.getSimpleName).getOrElse {
//      expr match {
//        case cast: CastExprMeta[_] =>
//          // different CAST operations have different costs, so we allow these to be configured
//          // based on the data types involved
//
//          // jgh: defaultExpressionCost doesn't exist in 23.06.0 version.
//          // set this value to 0.8
//          expr.conf.getGpuExpressionCost(s"Cast${cast.fromType}To${cast.toType}")
//            .getOrElse(0.8)
//        case _ =>
//          // many of our BaseExprMeta implementations are anonymous classes so we look directly at
//          // the wrapped expressions in some cases
//          expr.wrapped match {
//            case _: AttributeReference => 1.0 // no benefit on GPU
//            case Alias(_: AttributeReference, _) => 1.0 // no benefit on GPU
//            case _ => 0.8
//          }
//      }
//    }
//  }

}

object ModiMemoryCostHelper {
  private val GIGABYTE = 1024d * 1024d * 1024d

  /**
   * Calculate the cost (time) of transferring data at a given memory speed.
   *
   * @param dataSize Size of data to transfer, in bytes.
   * @param memorySpeed Memory speed, in GB/s.
   * @return Time in seconds.
   */
  def calculateCost(dataSize: Long, memorySpeed: Double): Double = {
    (dataSize / GIGABYTE) / memorySpeed
  }

  def isExcludedFromCost[INPUT <: Expression](expr: BaseExprMeta[INPUT]) = {
    expr.wrapped match {
      case _: WindowSpecDefinition | _: WindowFrame =>
        // Window expressions are Unevaluable and accessing dataType causes an exception
        true
      case _ => false
    }
  }

  def estimateGpuMemory(schema: StructType, rowCount: Double): Long = {
    // cardinality estimates tend to grow to very large numbers with nested joins so
    // we apply a maximum to the row count that we use when estimating data sizes in
    // order to avoid integer overflow
    val safeRowCount = rowCount.min(Int.MaxValue).toLong
    GpuBatchUtils.estimateGpuMemory(schema, safeRowCount)
  }

  def estimateGpuMemory(dataType: Option[DataType], nullable: Boolean, rowCount: Double): Long = {
    dataType match {
      case Some(dt) =>
        // cardinality estimates tend to grow to very large numbers with nested joins so
        // we apply a maximum to the row count that we use when estimating data sizes in
        // order to avoid integer overflow
        val safeRowCount = rowCount.min(Int.MaxValue).toLong
        GpuBatchUtils.estimateGpuMemory(dt, nullable, safeRowCount)
      case None =>
        throw new UnsupportedOperationException("Data type is None")
    }
  }
}

/**
 * Estimate the number of rows that an operator will output. Note that these row counts are
 * the aggregate across all output partitions.
 *
 * Logic is based on Spark's SizeInBytesOnlyStatsPlanVisitor. which operates on logical plans
 * and only computes data sizes, not row counts.
 */
object ModiRowCountPlanVisitor {

  def visit(plan: SparkPlanMeta[_]): Option[BigInt] = plan.wrapped match {
    case p: QueryStageExec =>
      p.getRuntimeStatistics.rowCount
    case _: GlobalLimitExec =>
      GlobalLimitShims.visit(plan.asInstanceOf[SparkPlanMeta[GlobalLimitExec]])
    case LocalLimitExec(limit, _) =>
      // LocalLimit applies the same limit for each partition
      val n = limit * plan.wrapped.asInstanceOf[SparkPlan]
        .outputPartitioning.numPartitions
      visit(plan.childPlans.head).map(_.min(n)).orElse(Some(n))
    case p: TakeOrderedAndProjectExec =>
      visit(plan.childPlans.head).map(_.min(p.limit)).orElse(Some(p.limit))
    case p: HashAggregateExec if p.groupingExpressions.isEmpty =>
      Some(1)
    case p: SortMergeJoinExec =>
      estimateJoin(plan, p.joinType)
    case p: ShuffledHashJoinExec =>
      estimateJoin(plan, p.joinType)
    case p: BroadcastHashJoinExec =>
      estimateJoin(plan, p.joinType)
    case _: UnionExec =>
      Some(plan.childPlans.flatMap(visit).sum)
    case _ =>
      default(plan)
  }

  private def estimateJoin(plan: SparkPlanMeta[_], joinType: JoinType): Option[BigInt] = {
    joinType match {
      case LeftAnti | LeftSemi =>
        // LeftSemi and LeftAnti won't ever be bigger than left
        visit(plan.childPlans.head)
      case _ =>
        default(plan)
    }
  }

  /**
   * The default row count is the product of the row count of all child plans.
   */
  private def default(p: SparkPlanMeta[_]): Option[BigInt] = {
    val one = BigInt(1)
    val sizePerRow = 72 // sej : size(b) of one Row
    val product = p.childPlans.map(visit)
      .filter(_.exists(_ > 0L))
      .map(_.get)
      .product
    if (product == one) {
      // product will be 1 when there are no child plans
      // sej : it means that plan reaches here is the first operation
      // sej : so, it should return current MB's row count.  
      // None
      val rowCount = SparkPlan.getCurrentMBSize / sizePerRow
      Some(BigInt(rowCount.toInt))
    } else {
      Some(product)
    }
  }
}

// jgh
// sealed abstract class Optimization
// abstract class Optimization

case class ModiAvoidTransition[INPUT <: SparkPlan](plan: SparkPlanMeta[INPUT])
  extends Optimization {
  override def toString: String = s"It is not worth moving to GPU for operator: " +
    s"${ModiExplain.format(plan)}"
}

case class ModiReplaceSection[INPUT <: SparkPlan](
    plan: SparkPlanMeta[INPUT],
    totalCpuCost: Double,
    totalGpuCost: Double) extends Optimization {
  override def toString: String = s"It is not worth keeping this section on GPU; " +
    s"gpuCost=$totalGpuCost, cpuCost=$totalCpuCost:\n${ModiExplain.format(plan)}"
}

object ModiExplain {

  def format(plan: SparkPlanMeta[_]): String = {
    plan.wrapped match {
      case p: SparkPlan => p.simpleString(SQLConf.get.maxToStringFields)
      case other => other.toString
    }
  }

  def formatTree(plan: SparkPlanMeta[_]): String = {
    val b = new StringBuilder
    formatTree(plan, b, "")
    b.toString
  }

  def formatTree(plan: SparkPlanMeta[_], b: StringBuilder, indent: String): Unit = {
    b.append(indent)
    b.append(format(plan))
    b.append('\n')
    plan.childPlans.filter(_.canThisBeReplaced)
      .foreach(child => formatTree(child, b, indent + "  "))
  }

}

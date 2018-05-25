/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package cs.ucla.edu.bwaspark.worker1

import cs.ucla.edu.bwaspark.datatype._

import scala.collection.mutable.MutableList
import java.util.TreeSet
import java.util.Comparator

import cs.ucla.edu.bwaspark.worker1.MemChain._
import cs.ucla.edu.bwaspark.worker1.MemChainFilter._
import cs.ucla.edu.bwaspark.worker1.MemChainToAlignBatched._
import cs.ucla.edu.bwaspark.worker1.MemSortAndDedup._
import cs.ucla.edu.bwaspark.util.LocusEncode._
import cs.ucla.edu.avro.fastq._
import cs.ucla.edu.bwaspark.debug.DebugFlag._
import edu.ucla.cs.cdsc.benchmarks.SWPipeline

//this standalone object defines the main job of BWA MEM:
//1)for each read, generate all the possible seed chains
//2)using SW algorithm to extend each chain to all possible aligns
object BWAMemWorker1Batched {
  SWPipeline.getSingleton.execute(null)

  /**
    * Perform BWAMEM worker1 function for single-end alignment
    * Alignment is processed in a batched way
    *
    * @param opt        the MemOptType object, BWAMEM options
    * @param bwt        BWT and Suffix Array
    * @param bns        .ann, .amb files
    * @param pac        .pac file (PAC array: uint8_t)
    * @param pes        pes array for worker2
    * @param seqArray   a batch of reads
    * @param numOfReads #reads in the batch
    *
    *                   Return: a batch of reads with alignments
    */
  def bwaMemWorker1Batched(opt: MemOptType, //BWA MEM options
                           bwt: BWTType, //BWT and Suffix Array
                           bns: BNTSeqType, //.ann, .amb files
                           pac: Array[Byte], //.pac file uint8_t
                           pes: Array[MemPeStat], //pes array
                           readArray: Array[Array[Byte]],
                           lenArray: Array[Int],
                           chainsFilteredArray: Array[Array[MemChainType]],
                           numOfReads: Int, //the number of the batched reads
                           runOnFPGA: Boolean, //if run on FPGA
                           threshold: Int //the batch threshold to run on FPGA
                          ): Array[ReadType] = { //all possible alignments for all the reads

    val readRetArray = new Array[ReadType](numOfReads)
    var i = 0;
    while (i < numOfReads) {
      readRetArray(i) = new ReadType
      //readRetArray(i).seq = seqArray(i)
      i = i + 1
    }

    val preResultsOfSW = new Array[Array[SWPreResultType]](numOfReads)
    val numOfSeedsArray = new Array[Int](numOfReads)
    val regArrays = new Array[MemAlnRegArrayType](numOfReads)
    i = 0;
    while (i < numOfReads) {
      if (chainsFilteredArray(i) == null) {
        preResultsOfSW(i) = null
        numOfSeedsArray(i) = 0
        regArrays(i) = null
      }
      else {
        preResultsOfSW(i) = new Array[SWPreResultType](chainsFilteredArray(i).length)
        var j = 0;
        while (j < chainsFilteredArray(i).length) {
          preResultsOfSW(i)(j) = calPreResultsOfSW(opt, bns.l_pac, pac, lenArray(i), readArray(i), chainsFilteredArray(i)(j))
          j = j + 1
        }
        numOfSeedsArray(i) = 0
        chainsFilteredArray(i).foreach(chain => {
          numOfSeedsArray(i) += chain.seedsRefArray.length
        })
        if (debugLevel == 1) println("Finished the calculation of pre-results of Smith-Waterman")
        if (debugLevel == 1) println("The number of reads in this pack is: " + numOfReads)
        regArrays(i) = new MemAlnRegArrayType
        regArrays(i).maxLength = numOfSeedsArray(i)
        regArrays(i).regs = new Array[MemAlnRegType](numOfSeedsArray(i))
      }
      i = i + 1;
    }
    if (debugLevel == 1) println("Finished the pre-processing part")


    memChainToAlnBatched(opt, bns.l_pac, pac, lenArray, readArray, numOfReads, preResultsOfSW, chainsFilteredArray, regArrays, runOnFPGA, threshold)
    if (debugLevel == 1) println("Finished the batched-processing part")
    regArrays.foreach(ele => {
      if (ele != null) ele.regs = ele.regs.filter(r => (r != null))
    })
    regArrays.foreach(ele => {
      if (ele != null) ele.maxLength = ele.regs.length
    })
    //i = 0;
    //while (i < numOfReads) {
    //  if (regArrays(i) == null) readRetArray(i).regs = null
    //  else readRetArray(i).regs = memSortAndDedup(regArrays(i), opt.maskLevelRedun).regs
    //  i = i+1
    //}
    readRetArray
  }

  /**
    * Perform BWAMEM worker1 function for pair-end alignment
    *
    * @param opt      the MemOptType object, BWAMEM options
    * @param bwt      BWT and Suffix Array
    * @param bns      .ann, .amb files
    * @param pac      .pac file (PAC array: uint8_t)
    * @param pes      pes array for worker2
    * @param pairSeqs a read with both ends
    *
    *                 Return: a read with alignments on both ends
    */
  def pairEndBwaMemWorker1Batched(opt: MemOptType, //BWA MEM options
                                  bwt: BWTType, //BWT and Suffix Array
                                  bns: BNTSeqType, //.ann, .amb files
                                  pac: Array[Byte], //.pac file uint8_t
                                  pes: Array[MemPeStat], //pes array
                                  seqArray: Array[String], //the pairs of seqs
                                  numOfReads: Int, //the number of reads in each batch
                                  runOnFPGA: Boolean, //if run on FPGA
                                  threshold: Int, //the batch threshold to run on FPGA
                                  jniSWExtendLibPath: String = null // SWExtend Library Path
                                 ): Array[PairEndReadType] = { //all possible alignment
    if (jniSWExtendLibPath != null && runOnFPGA)
      System.load(jniSWExtendLibPath)

    val readArray0 = new Array[Array[Byte]](numOfReads)
    val lenArray0 = new Array[Int](numOfReads)
    val chainsFilteredArray0 = new Array[Array[MemChainType]](numOfReads)
    val readArray1 = new Array[Array[Byte]](numOfReads)
    val lenArray1 = new Array[Int](numOfReads)
    val chainsFilteredArray1 = new Array[Array[MemChainType]](numOfReads)

    def parseStr(str: String) = {
      val tokens = str.split(' ')
      // first number: number of chains for the first read
      var idx = 0
      var numChains0 = tokens(idx).toInt
      idx += 1
      var chains0: Array[MemChainType] = null
      if (numChains0 > 0) chains0 = new Array[MemChainType](numChains0)
      // traverse each chain, get seeds
      for (i <- 0 until numChains0) {
        // the first number is pos
        chains0(i) = new MemChainType(tokens(idx).toLong, null)
        idx += 1
        // then the number of seeds
        var numSeeds = tokens(idx).toInt
        idx += 1
        chains0(i).seedsRefArray = new Array[MemSeedType](numSeeds)
        for (j <- 0 until numSeeds) {
          chains0(i).seedsRefArray(j) = new MemSeedType(tokens(idx).toLong, tokens(idx + 1).toInt, tokens(idx + 2).toInt)
          idx += 3
        }
      }
      var seqLen0 = tokens(idx).toInt
      idx += 1
      var read0: Array[Byte] = tokens(idx).getBytes
      idx += 1
      assert(read0.size == seqLen0)

      var numChains1 = tokens(idx).toInt
      idx += 1
      var chains1: Array[MemChainType] = null
      if (numChains1 > 0) chains1 = new Array[MemChainType](numChains1)
      // traverse each chain, get seeds
      for (i <- 0 until numChains1) {
        // the first number is pos
        chains1(i) = new MemChainType(tokens(idx).toLong, null)
        idx += 1
        // then the number of seeds
        var numSeeds = tokens(idx).toInt
        idx += 1
        chains1(i).seedsRefArray = new Array[MemSeedType](numSeeds)
        for (j <- 0 until numSeeds) {
          chains1(i).seedsRefArray(j) = new MemSeedType(tokens(idx).toLong, tokens(idx + 1).toInt, tokens(idx + 2).toInt)
          idx += 3
        }
      }
      var seqLen1 = tokens(idx).toInt
      idx += 1
      var read1: Array[Byte] = tokens(idx).getBytes
      idx += 1
      assert(read1.size == seqLen1)

      assert(idx == tokens.size)

      (read0, seqLen0, chains0, read1, seqLen1, chains1)
    }

    var i = 0
    while (i < numOfReads) {
      var ret = parseStr(seqArray(i))
      readArray0(i) = ret._1
      lenArray0(i) = ret._2
      chainsFilteredArray0(i) = ret._3
      readArray1(i) = ret._4
      lenArray1(i) = ret._5
      chainsFilteredArray1(i) = ret._6
      i = i + 1
    }

    val readRetArray0 = bwaMemWorker1Batched(opt, bwt, bns, pac, pes, readArray0, lenArray0, chainsFilteredArray0, numOfReads, runOnFPGA, threshold)
    val readRetArray1 = bwaMemWorker1Batched(opt, bwt, bns, pac, pes, readArray1, lenArray1, chainsFilteredArray1, numOfReads, runOnFPGA, threshold)
    var pairEndReadArray = new Array[PairEndReadType](numOfReads)
    i = 0
    while (i < numOfReads) {
      pairEndReadArray(i) = new PairEndReadType
      //pairEndReadArray(i).seq0 = readArray0(i).seq
      pairEndReadArray(i).regs0 = readRetArray0(i).regs
      //pairEndReadArray(i).seq1 = readArray1(i).seq
      pairEndReadArray(i).regs1 = readRetArray1(i).regs
      i = i + 1
    }

    pairEndReadArray // return
  }
}

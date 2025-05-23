package org.example

import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.ArrowFileWriter

import java.io.{File, FileOutputStream, IOException}
import java.util.Date

object LargeCsvReader extends App{
  var allocator: BufferAllocator = new RootAllocator(Long.MaxValue)
  var options = new ScanOptions(/*batchSize*/ 2<<17)
  val hugecsvFilePath = "jhu-usc.edu_PANCAN_HumanMethylation450.betaValue_whitelisted.tsv.synapse_download_5096262.xena.gz"
  var datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault, FileFormat.CSV, hugecsvFilePath)
  var dataset = datasetFactory.finish()
  var scanner = dataset.newScan(options);
  var reader = scanner.scanBatches()

  try {
    var totalBatchSize = 0
    var cnt:Long = 0
    val start = new Date().getTime
    var root = reader.getVectorSchemaRoot
    var file = new File("huge_randon_access_to_file.arrow")
    var fileOutputStream = new FileOutputStream(file)
    var writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel)
    writer.start()
    writer.writeBatch()
    while (reader.loadNextBatch())  {
      root = reader.getVectorSchemaRoot
      try {
        totalBatchSize += root.getRowCount
        cnt+=allocator.getAllocatedMemory
        writer.writeBatch()
        root.close()
        if (totalBatchSize%(1024*10)==0)
          println("Write "+cnt/1024/1024/1024+"GB now.")
      }

  }
    writer.end()
    val end = new Date().getTime
    println("Write "+cnt/1024/1024/1024+"GB in total, use "+(end - start) / 1000.0+"s.")
  }
  catch{
    case e: IOException => e.printStackTrace()
  }
  finally {
    scanner.close()
    allocator.close()
  }
}

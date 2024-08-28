package org.example.io.senai.istic

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.kotlin.client.MongoClient
import org.bson.Document
import org.bson.types.ObjectId
import org.example.io.senai.istic.ext.toDocument
import org.example.io.senai.istic.ext.toHexString
import org.example.io.senai.istic.hashmatrix.HashMatrix
import org.example.io.senai.istic.hashmatrix.StatelessHashMatrix
import org.example.io.senai.istic.io.CsvUtils
import org.example.printHashMatrixComparison
import java.io.File
import java.io.FileReader
import java.security.MessageDigest

fun main2() {
    val statelessHashMatrix0 = StatelessHashMatrix(MessageDigest.getInstance("SHA256"))

    val row0 = statelessHashMatrix0.addRow("row0")
    row0.putValue("col0".toByteArray())

    val row1 = statelessHashMatrix0.addRow("row1")
    row1.putValue("col0".toByteArray())

    println("hashMatrix0.hash: ${statelessHashMatrix0.currentHash.toHexString()}")
    println("row0.hash: ${row0.currentHash.toHexString()}")
    println("row1.hash: ${row1.currentHash.toHexString()}")

    val statelessHashMatrix1 = StatelessHashMatrix(MessageDigest.getInstance("SHA256"))

    val row0HM1 = statelessHashMatrix1.addRow("row0")
    row0HM1.putValue("col0".toByteArray())

    val row1HM1 = statelessHashMatrix1.addRow("row1")
    row1HM1.putValue("col0".toByteArray())

    println("hashMatrix0.hash: ${statelessHashMatrix0.currentHash.toHexString()}")
    println("row0HM1.hash: ${row0HM1.currentHash.toHexString()}")
    println("row1HM1.hash: ${row1HM1.currentHash.toHexString()}")

    val difference = statelessHashMatrix0.compare(statelessHashMatrix1)
    println("Has differences: ${difference.hasDifferences()}")

}


fun main() {
    loadHashMatrixFromCsv()
}

fun loadHashMatrixFromCsv() {
    val messageDigester = MessageDigest.getInstance("SHA256")
    val hashMatrices = mutableListOf<StatelessHashMatrix>()
    for (i in 1..200) {
        hashMatrices.add(csvToHashMatrix(File("C:\\tmp\\customers-10000.csv"), messageDigester, "Index"))
    }
    val difference = hashMatrices[3].compare(hashMatrices[133])
    println()
}

fun fetchHashMatrix() {
    val uri = "mongodb://localhost:27017"
    val settings = MongoClientSettings.builder()
        .applyConnectionString(ConnectionString(uri))
        .retryWrites(true)
        .build()

    val mongoClient = MongoClient.create(settings)
    val database = mongoClient.getDatabase("tests")
    val collection = database.getCollection<Document>("hashmatrices")

    val matrices = mutableListOf<Document>()

    val filter = Document()
    filter.append("_id", ObjectId("66cf32d60cc1711500a54a66"))
    for (i in 1..200) {
        val result = collection.find(filter).firstOrNull() ?: throw Exception("Could not find hash matrix with $filter")
        matrices.add(result)
    }
    println()
}

fun storeHashMatrix() {
    var start = System.currentTimeMillis()
    val csvHashMatrix = csvToHashMatrix(File("C:\\tmp\\customers-10000.csv"), MessageDigest.getInstance("SHA256"), "Index")

    val uri = "mongodb://localhost:27017"
    val settings = MongoClientSettings.builder()
        .applyConnectionString(ConnectionString(uri))
        .retryWrites(true)
        .build()

    val mongoClient = MongoClient.create(settings)
    val database = mongoClient.getDatabase("tests")

    val collection = database.getCollection<Document>("hashmatrices")

    val document = csvHashMatrix.toDocument()
    collection.insertOne(document)

    println("Dont say goonight tonight")

}

fun compareHashMatrix() {
    // CSV 1
    var start = System.currentTimeMillis()
    val csvHashMatrix = csvToHashMatrix(File("C:\\tmp\\customers-100000.csv"), MessageDigest.getInstance("SHA256"), "Index")
    println("1 - Took ${System.currentTimeMillis() - start}")

    // CSV 2
    start = System.currentTimeMillis()
    val csv2HashMatrix = csvToHashMatrix(File("C:\\tmp\\customers-100000-2.csv"), MessageDigest.getInstance("SHA256"), "Index")
    println("2 - Took ${System.currentTimeMillis() - start}")

    // Calculate hashes
    start = System.currentTimeMillis()
    csvHashMatrix.currentHash
    csv2HashMatrix.currentHash
    println("2.1 - Took ${System.currentTimeMillis() - start}")

    // DIFF
    start = System.currentTimeMillis()
    val difference = csvHashMatrix.compare(csv2HashMatrix)
    println("3 - Took ${System.currentTimeMillis() - start}")

    println("Has differences: ${difference.hasDifferences()}")
    printHashMatrixComparison(difference)
}

fun csvToHashMatrix(csvFile: File, messageDigester: MessageDigest, fieldInCsvUsedAsId: String? = null): StatelessHashMatrix {
    val csv = CsvUtils.readCsvWithNamedFields(FileReader(csvFile), useFirstLineAsFieldNames = true)
    val hashMatrix = StatelessHashMatrix(messageDigester)
    var rowIndex = -1
    for (line in csv) {
        rowIndex++

        // set the rowId as the an field in csv line or the rowIndex if not defined
        val rowId = if (fieldInCsvUsedAsId != null) line[fieldInCsvUsedAsId] else rowIndex.toString()
        // assert that the rowId was defined
        if (rowId == null) {
            throw Exception("rowId \"${fieldInCsvUsedAsId}\" was not found on line $line")
        }

        val row = hashMatrix.addRow(rowId)
        for (field in line) {
            row.putValue(field.value.toByteArray())
        }
    }

    return hashMatrix
}
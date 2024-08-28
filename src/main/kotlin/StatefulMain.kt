package org.example

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.kotlin.client.MongoClient
import org.bson.Document
import org.example.io.senai.istic.ext.toDocument
import org.example.io.senai.istic.hashmatrix.HashMatrix
import org.example.io.senai.istic.hashmatrix.HashMatrixComparison
import org.example.io.senai.istic.hashmatrix.StatelessHashMatrix
import org.example.io.senai.istic.hashmatrix.StatelessHashMatrixComparison
import org.example.io.senai.istic.io.CsvUtils
import java.io.File
import java.io.FileReader
import java.security.MessageDigest


fun main() {
    storeHashMatrix()
    println("Done")
}

fun storeHashMatrix() {
    var start = System.currentTimeMillis()
    val csvHashMatrix = csvToHashMatrix(File("C:\\tmp\\customers-100000.csv"), MessageDigest.getInstance("SHA256"), "Index")

    val uri = "mongodb://localhost:27017"
    val settings = MongoClientSettings.builder()
        .applyConnectionString(ConnectionString(uri))
        .retryWrites(true)
        .build()

    val mongoClient = MongoClient.create(settings)
    val database = mongoClient.getDatabase("hashmatrix")

    val collection = database.getCollection<Document>("hashmatrix")

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

fun printCsv(csv: List<Map<String, String>>) {
    var lineIndex = -1
    for (line in csv) {
        lineIndex++
        print("$lineIndex: ")
        for (entry in line) {
            print("${entry.key}=${entry.value}; ")
        }
        println()
    }
}

fun printHashMatrixComparison(difference: StatelessHashMatrixComparison) {
    println("Has differences: ${difference.hasDifferences()}")
    for (diff in difference.rowsWithDifferences) {
        println("Row ${diff.referenceRow.rowId} with diff: ${diff.difference}")
    }
}


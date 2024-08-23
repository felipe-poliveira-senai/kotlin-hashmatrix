package org.example

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.example.io.senai.istic.hashmatrix.HashMatrix
import org.example.io.senai.istic.hashmatrix.HashMatrixComparison
import org.example.io.senai.istic.io.CsvUtils
import java.io.File
import java.security.MessageDigest


fun main() = runBlocking {
    // CSV 1
    var start = System.currentTimeMillis()
    val csv = CsvUtils.readCsvWithNamedFields(File("C:\\tmp\\customers-1000000.csv").readText(), useFirstLineAsFieldNames = true)
//    val csv = CsvUtils.readCsvWithNamedFields(File("C:\\tmp\\customers-1000000.csv").readText(), useFirstLineAsFieldNames = true)
    println("1 - Took ${System.currentTimeMillis() - start}")

    start = System.currentTimeMillis()
    val csvHashMatrix = csvToHashMatrix(csv, {MessageDigest.getInstance("SHA256")}, "Index")
    println("2 - Took ${System.currentTimeMillis() - start}")

    // CSV 2
    start = System.currentTimeMillis()
    val csv2 = CsvUtils.readCsvWithNamedFields(File("C:\\tmp\\customers-100-2.csv").readText(), useFirstLineAsFieldNames = true)
//    val csv2 = CsvUtils.readCsvWithNamedFields(File("C:\\tmp\\customers-1000000.csv").readText(), useFirstLineAsFieldNames = true)
    println("3 - Took ${System.currentTimeMillis() - start}")

    start = System.currentTimeMillis()
    val csv2HashMatrix = csvToHashMatrix(csv2, {MessageDigest.getInstance("SHA256")}, "Index")
    println("4 - Took ${System.currentTimeMillis() - start}")

    start = System.currentTimeMillis()
    val difference = csvHashMatrix.compare(csv2HashMatrix)
    println("5 - Took ${System.currentTimeMillis() - start}")
}

fun csvToHashMatrix(csv: List<Map<String, String>>, messageDigestSeeder: () -> MessageDigest, fieldInCsvUsedAsId: String? = null): HashMatrix {
    val hashMatrix = HashMatrix(messageDigestSeeder)
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
            row.putValue(field.key, field.value.toByteArray())
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

fun printHashMatrixComparison(difference: HashMatrixComparison) {
    println("Has differences: ${difference.hasDifferences()}")
    for (diff in difference.rowsWithDifferences) {
        println("Row ${diff.referenceRow.rowId} with diff: ${diff.difference}")
    }
}


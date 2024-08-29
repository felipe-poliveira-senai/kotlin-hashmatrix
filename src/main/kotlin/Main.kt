import io.senai.istic.HashMatrix
import io.senai.istic.HashMatrixBuilder
import io.senai.istic.HashMatrixComparison
import org.example.io.senai.istic.ext.toHexString
import java.io.File
import java.io.FileReader
import java.security.MessageDigest

fun main() {
    var start = System.currentTimeMillis()
    val hm0 = HashMatrixBuilder.fromCsv(
        FileReader(File("C:\\tmp\\customers-100000.csv")),
        { MessageDigest.getInstance("SHA256")},
        useColumnIndexAsIdentifier = 1,
        ).build()
    println("Took ${System.currentTimeMillis() - start} to generate csv")

    start = System.currentTimeMillis()
    val hm1 = HashMatrixBuilder.fromCsv(
        FileReader(File("C:\\tmp\\customers-100000-2.csv")),
        { MessageDigest.getInstance("SHA256")},
        useColumnIndexAsIdentifier = 1,
    ).build()
    println("Took ${System.currentTimeMillis() - start} to generate csv")

    start = System.currentTimeMillis()
    val diff = hm0.compare(hm1)
    println("Has differences: ${diff.hasDifferences()}")
    for ((rowId, rowWithDiff) in diff.rowsWithDifferences) {
        println("\trowDiff[$rowId]=${rowWithDiff}")
    }
    println("Took ${System.currentTimeMillis() - start} to compare matrices")
}

fun main2() {
    val hm0 = hashMatrix(30_000, 150)
    val hm1 = hashMatrix(30_000, 150)
    val diff = compareMatrices(hm0, hm1)
    println("rowsWithDifferences=${diff.rowsWithDifferences.count()}")
}

fun compareMatrices(matrixA: HashMatrix, matrixB: HashMatrix): HashMatrixComparison {
    val ttm = System.currentTimeMillis()
    val diff = matrixA.compare(matrixB)
    println("rowsWithDifferences=${diff.rowsWithDifferences.count()}")
    println("Took ${System.currentTimeMillis() - ttm} to compare matrices")
    return diff
}

fun hashMatrix(numberOfRows: Int, numberOfColumns: Int): HashMatrix {
    val hashMatrixBuilder = HashMatrixBuilder { MessageDigest.getInstance("SHA256") }
    var ttm = System.currentTimeMillis()

    // add row
    for (ri in 0..<numberOfRows) {
        val row = hashMatrixBuilder.addRow("r$ri")

        // add columns
        for (ci in 0..<numberOfColumns) {
//            if ((System.currentTimeMillis() % 2).toInt() == 0) {
//                row.addValue("par-$ci".toByteArray())
//            }
//            else {
//                row.addValue("impar-$ci".toByteArray())
//            }
            row.addValue("$ci".toByteArray())
        }
    }
    println("Took ${System.currentTimeMillis() - ttm} to create the rows")

    ttm = System.currentTimeMillis()
    val hashMatrix = hashMatrixBuilder.build()
    println("Took ${System.currentTimeMillis() - ttm} to build the hash matrix")

    return hashMatrix
}
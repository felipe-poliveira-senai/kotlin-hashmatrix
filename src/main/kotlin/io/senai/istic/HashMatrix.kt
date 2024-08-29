package io.senai.istic

import org.example.io.senai.istic.io.CsvUtils.Companion.readCsvLine
import java.io.BufferedReader
import java.io.Reader
import java.security.MessageDigest



class HashMatrixBuilder(
    /**
     * A callback that create MessageDigest instances that will be used into the hash matrix structures
     */
    private val messageDigesterSeeder: () -> MessageDigest
) {

    companion object {
        fun fromCsv(
            input: Reader,
            messageDigesterSeeder: () -> MessageDigest,
            fieldDelimiter: Char = ',',
            skipFirstLine: Boolean = false,
            useColumnIndexAsIdentifier: Int? = null,
        ): HashMatrixBuilder {

            val reader = BufferedReader(input)
            val hashMatrixBuilder = HashMatrixBuilder(messageDigesterSeeder)

            reader.useLines { lines ->
                lines.forEachIndexed { lineIndex, line ->

                    if (skipFirstLine && lineIndex == 0) {
                        return@forEachIndexed
                    }

                    val fields = readCsvLine(line, fieldDelimiter)

                    // The rowId will be:
                    val rowId = if (useColumnIndexAsIdentifier == null) lineIndex.toString()
                    else if (useColumnIndexAsIdentifier < fields.count()) fields[useColumnIndexAsIdentifier]
                    else throw Exception("useColumnIndexAsIdentifier is defined as $useColumnIndexAsIdentifier and the " +
                            "fields count is ${fields.count()} so the index is out of bounds from the fields: $fields")

                    val row = hashMatrixBuilder.addRow(rowId)
                    for (field in fields) {
                        row.addValue(field.toByteArray())
                    }
                }
            }

            return hashMatrixBuilder
        }
    }

    /**
     * Store the reference for the current working row. The current working row
     * is the last added row into the HashMatrixBuilder. This variable is used to control
     * possible de-synchronization when for example `val r0 = matrixBuilder.addRow('r0')` and
     * `val r1 = matrixBuilder.addRow('r1')` and we try to `r0.addValue()` after adding `r1`
     */
    private var currentWorkingRow: HashMatrixRowBuilder? = null

    /**
     * Store the message digester for the matrix
     */
    private val matrixMessageDigester = messageDigesterSeeder()

    /**
     * Store all the hash matrix rows
     */
    private val rows = mutableMapOf<String, HashMatrixRowBuilder>()

    /**
     * Add a row into the hash matrix
     */
    fun addRow(rowId: String): HashMatrixRowBuilder {
        // If the 'rowId' is already defined throw a exception
        if (rows.contains(rowId)) {
            throw Exception("There is already a row with id $rowId in this hash matrix")
        }

        val newRow = HashMatrixRowBuilder(this, messageDigesterSeeder())
        rows[rowId] = newRow
        currentWorkingRow = newRow
        return newRow
    }

    /**
     * Add a value to be digested by the matrix message digester. This method is private
     * as it should be called by HashMatrixRowBuilder.addValue()
     */
    private fun addValue(row: HashMatrixRowBuilder, newValue: ByteArray) {
        // validate the given row is the same as  current working row
        if (currentWorkingRow != null && currentWorkingRow != row) {
            throw Exception("Operation not allowed. An old row was trying to update the matrix hash after a new row was added into it")
        }

        // digest the newValue
        matrixMessageDigester.update(newValue)
    }

    /**
     * Build a HashMatrix based on the data of the HashMatrixBuilder
     */
    fun build(): HashMatrix {
        val matrixHash = this.matrixMessageDigester.digest()
        val rowHashes = mutableMapOf<String, ByteArray>()

        // load rowHashes
        for ((rowBuilderId, rowBuilder) in rows) {
            rowHashes[rowBuilderId] = rowBuilder.rowMessageDigester.digest()
        }

        return HashMatrix(matrixMessageDigester.algorithm, matrixHash, rowHashes)
    }

    class HashMatrixRowBuilder(
        /**
         * Store a reference to the HashMatrixBuilder that created this row
         */
        private val matrix: HashMatrixBuilder,

        /**
         * The message digester that will digest the values of the hash matrix row
         */
        internal val rowMessageDigester: MessageDigest
    ) {
        /**
         * Add a value to be digested by the row message digester
         */
        fun addValue(newValue: ByteArray) {
            rowMessageDigester.update(newValue)
            matrix.addValue(this, newValue)
        }
    }

}

/**
 *
 */
class HashMatrix(
    /**
     * The algorithm used to calculate hashes
     */
    val hashAlgorithm: String,
    /**
     * The matrix hash. The matrix hash represents the digest of all data provided in the
     * hash matrix rows.
     */
    val matrixHash: ByteArray,

    /**
     * The hashes of each row of the matrixHash
     */
    val rowHashes: Map<String, ByteArray>
) {
    /**
     * Compare this hash matrix with other hash matrix. This instance will be used as reference
     */
    fun compare(otherHashMatrix: HashMatrix) = HashMatrixComparison(this, otherHashMatrix)
}

class HashMatrixComparison(
    /**
     * The Hash Matrix used as reference
     */
    private val referenceHashMatrix: HashMatrix,
    /**
     * The compared Hash Matrix
     */
    private val comparedHashMatrix: HashMatrix
) {

    /**
     * Store the rows with differences
     */
    val rowsWithDifferences: MutableMap<String, HashMatrixDifference> = mutableMapOf()

    /**
     * Return a flag indicating if the matrices has differences
     */
    fun hasDifferences() = rowsWithDifferences.isNotEmpty()

    init {
        lookForDifferences()
    }

    /**
     * Look for differences between two hash matrices
     */
    private fun lookForDifferences() {
        // ignore  if the matrices are identical (with same hash)
        if (referenceHashMatrix.matrixHash.contentEquals(comparedHashMatrix.matrixHash)) {
            return
        }

        for ((refRowId, refRowHash) in referenceHashMatrix.rowHashes) {

            // get the compared row using the reference row id...
            val comparedRowHash = comparedHashMatrix.rowHashes[refRowId]

            // if the compared row does not exist considered it REMOVED
            if (comparedRowHash == null) {
                rowsWithDifferences[refRowId] = HashMatrixDifference.REMOVED
                continue
            }

            // if the hash of the compared row does not match with the hash of the reference row considered it changed
            if (!comparedRowHash.contentEquals(refRowHash)) {
                rowsWithDifferences[refRowId] = HashMatrixDifference.CHANGED
            }
        }

        // all rows contained int he compared hash matrix that does not exists in the reference
        // is considered ADDED
        for ((comparedRowId, _) in comparedHashMatrix.rowHashes) {
            if (!referenceHashMatrix.rowHashes.containsKey(comparedRowId)) {
                rowsWithDifferences[comparedRowId] = HashMatrixDifference.ADDED
            }
        }
    }
}

enum class HashMatrixDifference {
    ADDED,
    CHANGED,
    REMOVED,
}
package org.example.io.senai.istic.hashmatrix

import java.security.MessageDigest
import java.util.concurrent.Semaphore

class StatelessHashMatrix(
    /**
     * Function that create a message digester function
     */
    private val messageDigesterSeeder: () -> MessageDigest
) {

    companion object {
        /**
         * Parse an Array<HashMatrixSerializer> into a HashMatrix
         */
        fun fromHashMatrixSerdeCollection(data: Array<HashMatrixSerializer>, messageDigesterSeeder: () -> MessageDigest): HashMatrix {
            val matrix = HashMatrix(messageDigesterSeeder)
            for (row in data) {
                val matrixRow = matrix.addRow(row.hashMatrixRowIdentifier())
                for (col in row.serialize()) {
                    matrixRow.putValue(col.first, col.second)
                }
            }
            return matrix
        }
    }

    /**
     * Semaphore that controls access to hash calculation function
     */
    private val hashCalculationSemaphore: Semaphore = Semaphore(1)

    /**
     * Store the matrix total hash
     */
    private var matrixHash: ByteArray? = null

    /**
     * Store the instance that calculates the hash for the matrix
     */
    private val messageDigester = messageDigesterSeeder()

    /**
     * Store the rows hashes
     */
    private val rows: MutableMap<String, StatelessHashMatrixRow> = mutableMapOf()

    /**
     * Returns a copy-safe instance of row hashes
     */
    val rowHashes
        get() = rows

    /**
     * Add and return a HashMatrixRow associated with this HashMatrix
     */
    fun addRow(rowId: String): StatelessHashMatrixRow {
        // assert that is not duplicated
        if (this.rows.containsKey(rowId)) {
            throw Exception("There is already a key \"${rowId}\" added into the HashMatrix")
        }

        // create, add in the collection and return a new HashMatrixRow
        val row = StatelessHashMatrixRow(this, rowId, messageDigesterSeeder)
        this.rows[rowId] = row
        return row
    }

    val currentHash: ByteArray
        get() {
            return matrixHash ?: throw Exception("Matrix hash was never defined. Use .addRow().putValue() before calling this method")
        }

    /**
     * Compare this instance of HashMatrix and compare it with other matrix
     */
    fun compare(otherMatrix: StatelessHashMatrix, comparisonMode: ComparisonMode = ComparisonMode.Shallow) = StatelessHashMatrixComparison(this, otherMatrix, comparisonMode)

    /**
     * Get a row by its id. If the row does not exist returns null
     */
    fun getRowById(rowId: String): StatelessHashMatrixRow? = rows[rowId]

    /**
     * Returns a read-only instance of the rows
     */
    fun rows() = rows.toMap()

    fun updateHash(newDigestedValue: ByteArray) {
        matrixHash = if (matrixHash == null) messageDigester.digest(newDigestedValue) else messageDigester.digest(matrixHash?.plus(newDigestedValue))
    }
}

class StatelessHashMatrixRow(
    /**
     * The matrix that create the row
     */
    private val matrix: StatelessHashMatrix,
    /**
     * The identifier of the HashMatrixRow
     */
    val rowId: String,

    /**
     * The message digester instance used in the row
     */
    private val messageDigesterSeeder: () -> MessageDigest,
) {


    val currentHash: ByteArray
        get() = rowHash ?: throw Exception("Could not fetch currentHash from row because not value was add into it. Use .putValue before calling this method")

    /**
     * Semaphore that controls access to hash calculation function
     */
    private val hashCalculationSemaphore: Semaphore = Semaphore(1)

    private val messageDigester = messageDigesterSeeder()

    /**
     * Store the row message digester seeder
     */
    private var rowHash: ByteArray? = null

    /**
     *
     */
    fun putValue(value: ByteArray) {

        // create a new instance of the message digester for the new value and store it on the map
        val newValueMessageDigester = messageDigesterSeeder()
        val digestedValue = newValueMessageDigester.digest(value)

        // update the hash from the row and from the matrix
        rowHash = if (rowHash == null) messageDigester.digest(digestedValue) else messageDigester.digest(rowHash?.plus(digestedValue))
        matrix.updateHash(digestedValue)
    }

}

class StatelessHashMatrixComparison(
    /**
     * The matrix used as reference to be compared
     */
    val referenceMatrix: StatelessHashMatrix,
    /**
     * The compared hash matrix
     */
    val comparedMatrix: StatelessHashMatrix,
    /**
     * Check how the comparison algorithm will run.
     * If Shallow: Do not identify the changes between the row columns
     * Deep: Verify the differences between the columns
     */
    val comparisonMode: ComparisonMode
) {

    val rowsWithDifferences: MutableList<StatelessHashMatrixRow> = mutableListOf()

    init {
        lookForDifferences(comparisonMode)
    }

    /**
     * Look for all the differences between the reference and compared hash mastrix
     */
    private fun lookForDifferences(comparisonMode: ComparisonMode) {
        // ignore if the matrix is equal
        if (referenceMatrix.currentHash.contentEquals(comparedMatrix.currentHash)) {
            return;
        }

        // Iterate over each entry in the reference matrix
        for (referenceMatrixRowEntry in referenceMatrix.rows()) {

            val referenceMatrixRow = referenceMatrixRowEntry.value
            val comparedMatrixRow = comparedMatrix.getRowById(referenceMatrixRowEntry.key)

            // if the compared matrix row does not exist consider it removed
            if (comparedMatrixRow == null) {
                rowsWithDifferences.add(StatelessHashMatrixRow(referenceMatrixRow, Difference.Removed))
                continue
            }

            // if the reference matrix row does not match the compared matrix row considered it changed
            if (!referenceMatrixRow.currentHash.contentEquals(comparedMatrixRow.currentHash)) {
                rowsWithDifferences.add(StatelessHashMatrixRow(referenceMatrixRow, Difference.Changed))
                continue
            }
        }

        // Iterate over each entry in the compared matrix
        for (comparedMatrixRowEntry in comparedMatrix.rows()) {

            // if the reference matrix does not have the row from the compared matrix row consider it Added
            if (referenceMatrix.getRowById(comparedMatrixRowEntry.key) == null) {
                rowsWithDifferences.add(StatelessHashMatrixRow(comparedMatrixRowEntry.value, Difference.Added))
                continue
            }
        }
    }

    /**
     * Flag that indicates if differences has being detected in the hash matrix comparison
     */
    fun hasDifferences() = rowsWithDifferences.isNotEmpty()

    /**
     * Store information about hash matrix comparison differences
     */
    class StatelessHashMatrixRow(
        val referenceRow: org.example.io.senai.istic.hashmatrix.StatelessHashMatrixRow,
        val difference: Difference,
    )
}

enum class ComparisonMode {
    Shallow,
    Deep,
}

enum class Difference {
    Changed,
    Added,
    Removed,
}
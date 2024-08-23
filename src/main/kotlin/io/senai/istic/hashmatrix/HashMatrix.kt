package org.example.io.senai.istic.hashmatrix

import java.security.MessageDigest
import java.util.concurrent.Semaphore

class HashMatrix(
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
     * Store the rows hashes
     */
    private val rows: LinkedHashMap<String, HashMatrixRow> = LinkedHashMap();

    /**
     * Add and return a HashMatrixRow associated with this HashMatrix
     */
    fun addRow(rowId: String): HashMatrixRow {
        // assert that is not duplicated
        if (this.rows.containsKey(rowId)) {
            throw Exception("There is already a key \"${rowId}\" added into the HashMatrix")
        }

        // create, add in the collection and return a new HashMatrixRow
        val row = HashMatrixRow(this, rowId, messageDigesterSeeder)
        this.rows[rowId] = row
        return row
    }

    val currentHash: ByteArray
        get() {
            // lock other threads to calculate the hash simultaneously
            hashCalculationSemaphore.acquire()

            // if the row hash is defined return it
            if (matrixHash != null) {
                return matrixHash as ByteArray;
            }

            try {
                // check for valid amount of rows
                if (rows.isEmpty()) {
                    throw Exception("Could not access the currentHash from the HashMatrix as no value is present into it. " +
                            "Use .addRow().addValue() before calling this method")
                }

                // calculate the rowHash
                val messageDigester = messageDigesterSeeder()
                for (rowEntry in rows) {
                    val digestedValue = rowEntry.value.currentHash
                    matrixHash = messageDigester.digest(matrixHash?.plus(digestedValue) ?: digestedValue)
                }

            } finally {
                // release lock after calculation
                hashCalculationSemaphore.release()
            }

            return matrixHash as ByteArray
        }

    /**
     * Compare this instance of HashMatrix and compare it with other matrix
     */
    fun compare(otherMatrix: HashMatrix, comparisonMode: ComparisonMode = ComparisonMode.Shallow) = HashMatrixComparison(this, otherMatrix, comparisonMode)

    /**
     * Get a row by its id. If the row does not exist returns null
     */
    fun getRowById(rowId: String): HashMatrixRow? = rows[rowId]

    /**
     * Returns a read-only instance of the rows
     */
    fun rows() = rows.toMap()

    fun resetHash() {
        matrixHash = null
    }
}

class HashMatrixRow(
    /**
     * The matrix that create the row
     */
    private val matrix: HashMatrix,
    /**
     * The identifier of the HashMatrixRow
     */
    val rowId: String,

    /**
     * The message digester instance used in the row
     */
    private val messageDigesterSeeder: () -> MessageDigest,
) {
    /**
     * Store the hashes of the hash matrix row
     */
    private val hashes: LinkedHashMap<String, ByteArray> = LinkedHashMap()

    /**
     * Store the row message digester seeder
     */
    private var rowHash: ByteArray? = null

    /**
     * Semaphore that controls access to hash calculation function
     */
    private val hashCalculationSemaphore: Semaphore = Semaphore(1)

    /**
     * Return a new instance of the hashes of the hash matrix row
     */
    fun hashes() = hashes.toMap()

    val currentHash: ByteArray
        get() {
            try {
                // If the row hash is defined return it
                if (rowHash != null) {
                    return rowHash as ByteArray;
                }

                // lock the semaphore
                hashCalculationSemaphore.acquire()

                // If after acquiring the lock of the semaphore another thread has already defined the rowHash
                // release the lock and return it without calculating it again
                if (rowHash != null) {
                    hashCalculationSemaphore.release()
                    return rowHash as ByteArray;
                }

                // if not values are passed to the HashMatrixRow
                if (hashes.isEmpty()) {
                    throw Exception("No value was added in the row \"${rowId}\". Use HashMatrixRow.putValue() before using this method")
                }

                // calculate the rowHash
                val messageDigester = messageDigesterSeeder()
                for (hashEntry in hashes) {
                    val digestedValue = hashEntry.value
                    rowHash = messageDigester.digest(rowHash?.plus(digestedValue) ?: digestedValue)
                }
            } finally {
                // release semaphore
                hashCalculationSemaphore.release()
            }

            return rowHash as ByteArray
        }

    /**
     * Return a column hash identified by the column id
     */
    fun getColumnById(columnId: String) = hashes[columnId]

    /**
     *
     */
    fun putValue(colId: String, value: ByteArray) {

        // Everytime the value is changed the hash should be calculated again
        matrix.resetHash()
        rowHash = null

        // create a new instance of the message digester for the new value and store it on the map
        val newValueMessageDigester = messageDigesterSeeder()
        val digestedValue = newValueMessageDigester.digest(value)
        hashes[colId] = digestedValue
    }

}

class HashMatrixComparison(
    /**
     * The matrix used as reference to be compared
     */
    val referenceMatrix: HashMatrix,
    /**
     * The compared hash matrix
     */
    val comparedMatrix: HashMatrix,
    /**
     * Check how the comparison algorithm will run.
     * If Shallow: Do not identify the changes between the row columns
     * Deep: Verify the differences between the columns
     */
    val comparisonMode: ComparisonMode
) {

    val rowsWithDifferences: MutableList<HashMatrixRowDifference> = mutableListOf()

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
                rowsWithDifferences.add(HashMatrixRowDifference(referenceMatrixRow, null, Difference.Removed, comparisonMode))
                continue
            }

            // if the reference matrix row does not match the compared matrix row considered it changed
            if (!referenceMatrixRow.currentHash.contentEquals(comparedMatrixRow.currentHash)) {
                rowsWithDifferences.add(HashMatrixRowDifference(referenceMatrixRow, comparedMatrixRow, Difference.Changed, comparisonMode))
                continue
            }
        }

        // Iterate over each entry in the compared matrix
        for (comparedMatrixRowEntry in comparedMatrix.rows()) {

            // if the reference matrix does not have the row from the compared matrix row consider it Added
            if (referenceMatrix.getRowById(comparedMatrixRowEntry.key) == null) {
                rowsWithDifferences.add(HashMatrixRowDifference(comparedMatrixRowEntry.value, null, Difference.Added, comparisonMode))
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
    class HashMatrixRowDifference(
        val referenceRow: HashMatrixRow,
        val comparedRow: HashMatrixRow?,
        val difference: Difference,
        val comparisonMode: ComparisonMode
    ) {
        /**
         * Store the column differences. This property will only add
         * values if the comparisonMode is 'Deep'
         */
        var columnsWithDifferences: MutableMap<String, Difference> = mutableMapOf()

         init {
             // only look for difference if comparison mode is 'Deep'
             if (comparisonMode == ComparisonMode.Deep) {
                 lookForDifferences()
             }
         }

        private fun lookForDifferences() {
            // Ignore if compared row is null
            if (comparedRow == null) {
                return
            }
            // Iterate over each column from the reference row
            for (colFromReferenceRowEntry in referenceRow.hashes()) {
                val colFromComparedRow = comparedRow.getColumnById(colFromReferenceRowEntry.key)

                // If the reference column does not exist in compared row mark it as 'Removed'
                if (colFromComparedRow == null) {
                    columnsWithDifferences[colFromReferenceRowEntry.key] = Difference.Removed
                    continue
                }

                // if the content from the reference column is different from the compared column mark is as 'Changed'
                if (!colFromReferenceRowEntry.value.contentEquals(colFromComparedRow)) {
                    columnsWithDifferences[colFromReferenceRowEntry.key] = Difference.Changed
                    continue
                }
            }

            // Iterate over each column from the compared row
            for (colFromComparedRowEntry in comparedRow.hashes()) {

                // if the reference row does not have a reference to the compared row column entry mark it as 'Added'
                if (referenceRow.getColumnById(colFromComparedRowEntry.key) == null) {
                    columnsWithDifferences[colFromComparedRowEntry.key] = Difference.Added
                    continue
                }
            }
        }
    }
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
package org.example.io.senai.istic.ext

import org.bson.Document
import org.example.io.senai.istic.hashmatrix.*
import java.util.Base64

fun StatelessHashMatrix.toDocument(): Document {
    val document = Document()
    document["matrixHash"] = this.currentHash
    val rowsDocument = Document()

    // include the
    for ((rowId, row) in this.rowHashes) {
        rowsDocument[rowId] = row.toDocument()
    }
    document["rows"] = rowsDocument
    return document
}

fun StatelessHashMatrixRow.toDocument(): Document {
    val rowDocument = Document()
    rowDocument["rowHash"] = this.currentHash
    return rowDocument
}
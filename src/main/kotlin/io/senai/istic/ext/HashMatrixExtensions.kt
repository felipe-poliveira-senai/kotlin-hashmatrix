package org.example.io.senai.istic.ext

import org.bson.Document
import org.example.io.senai.istic.hashmatrix.HashMatrix
import org.example.io.senai.istic.hashmatrix.HashMatrixRow
import java.util.Base64

fun HashMatrix.toDocument(): Document {
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

fun HashMatrixRow.toDocument(): Document {
    val rowDocument = Document()
    rowDocument["rowHash"] = this.currentHash

    // include the [hashes]
    // Crete a map where [key=hashId]=[value=base64(hash)]
    val hashesDocument = Document()
    for ((hashId, hash) in this.hashes()) {
        hashesDocument[hashId] = hash
    }
    rowDocument["hashes"] = hashesDocument

    return rowDocument
}
package org.example.io.senai.istic.hashmatrix

interface HashMatrixSerializer {


    /**
     * Return an identifier that should be used
     */
    fun hashMatrixRowIdentifier(): String

    /**
     * Serialize
     */
    fun serialize(): Array<Pair<String, ByteArray>>
}
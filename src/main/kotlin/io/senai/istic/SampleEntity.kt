package org.example.io.senai.istic

import org.example.io.senai.istic.hashmatrix.HashMatrixSerializer

class SampleEntity(
    /**
     * ID
     */
    val id: String,
    /**
     * The name of the entity
     */
    val name: String,
) : HashMatrixSerializer {


    override fun hashMatrixRowIdentifier() = id

    override fun serialize(): Array<Pair<String, ByteArray>> {
        return arrayOf(
            Pair("id", id.toByteArray()),
            Pair("name", name.toByteArray()),
        )
    }

}
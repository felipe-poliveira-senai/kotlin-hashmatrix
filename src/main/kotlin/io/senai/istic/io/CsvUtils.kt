package org.example.io.senai.istic.io

import java.io.BufferedReader
import java.io.Reader

class CsvUtils {
    companion object {

        fun readCsvWithNamedFields(
            input: Reader,
            fieldDelimiter: Char = ',',
            useFirstLineAsFieldNames: Boolean = false
        ): Sequence<Map<String, String>> {

            val reader = BufferedReader(input)
            var fieldNames: Array<String>? = null

            return sequence {
                reader.useLines { lines ->
                    lines.forEachIndexed { _, line ->
                        val fields = readCsvLine(line, fieldDelimiter)

                        if (fields.isEmpty()) {
                            return@forEachIndexed
                        }

                        if (useFirstLineAsFieldNames && fieldNames == null) {
                            fieldNames = fields
                            return@forEachIndexed
                        }

                        if (fieldNames != null && fieldNames!!.size < fields.size) {
                            throw Exception("The amount of field names used to identify the csv line fields is lesser than the number " +
                                    "of fields. Field names: ${fieldNames!!.joinToString(",")}; Fields: ${fields.joinToString(",")}")
                        }

                        val fieldsMap = fields.mapIndexed { fieldIndex, field ->
                            val fieldName = fieldNames?.getOrNull(fieldIndex) ?: fieldIndex.toString()
                            fieldName to field
                        }.toMap()

                        yield(fieldsMap)
                    }
                }

                reader.close()
            }
        }

        /**
         * Read a line that was derived from a .csv file
         */
        fun readCsvLine(line: String, fieldDelimiter: Char = ','): Array<String> {
            val csvFields = mutableListOf<String>()
            var strBuilder = StringBuilder()
            var curContext: CsvContext = CsvContext.UNKNOWN
            var onEscape = false

            // iterate over each char from the csv line
            for (curChar in line) {
                when(curContext) {
                    CsvContext.UNKNOWN -> {
                        when (curChar) {
                            fieldDelimiter -> {
                                continue
                            }
                            '"' -> {
                                curContext = CsvContext.ON_STRING_LITERAL
                            }
                            else -> {
                                if (!curChar.isWhitespace()) {
                                    curContext = CsvContext.SIMPLE_FIELD
                                    strBuilder.append(curChar)
                                }
                            }
                        }
                    }
                    CsvContext.ON_STRING_LITERAL -> {
                        // if the current context is 'onEscape'
                        if (onEscape) {
                            strBuilder.append(curChar)
                            onEscape = false
                        }
                        when (curChar) {
                            // found an escape character
                            '\\' -> {
                                onEscape = true
                            }
                            // found an end of string literal
                            '"' -> {
                                csvFields.add(strBuilder.toString())
                                strBuilder.clear()
                                curContext = CsvContext.OUT_OF_STRING_LITERAL
                            }
                            else -> {
                                strBuilder.append(curChar)
                            }
                        }
                    }
                    CsvContext.OUT_OF_STRING_LITERAL -> {
                        when (curChar) {
                            fieldDelimiter -> {
                                curContext = CsvContext.UNKNOWN
                            }
                        }
                    }
                    CsvContext.SIMPLE_FIELD -> {
                        when (curChar) {
                            fieldDelimiter -> {
                                csvFields.add(strBuilder.toString())
                                strBuilder.clear()
                                curContext = CsvContext.UNKNOWN
                            }
                            else -> {
                                strBuilder.append(curChar)
                            }
                        }
                    }
                }
            }

            // if there is content in the strBuilder add it onto it
            if (strBuilder.isNotEmpty()) {
                csvFields.add(strBuilder.toString())
            }

            return csvFields.toTypedArray()
        }
    }

    private enum class CsvContext {
        /**
         * The current context is on a STRING_LITERAL Example:
         * ``Adam Smith, 13, "Phantom street"` in that case: "Phantom Street"
         */
        ON_STRING_LITERAL,

        /**
         *
         */
        OUT_OF_STRING_LITERAL,
        /**
        *  The current context is on a simple field. Example:
         *  `Adam Smith, 13, "Phantom street"` in that case: "Adam Smith" or  "13"
        */
        SIMPLE_FIELD,
        /**
         * The csv context is unknown to the interpreter
         */
        UNKNOWN,
    }
}
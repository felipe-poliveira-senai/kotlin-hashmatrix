package org.example.io.senai.istic.io

class CsvUtils {
    companion object {

        fun readCsvWithNamedFields(
            content: String,
            fieldDelimiter: Char = ',',
            lineDelimiter: String = System.lineSeparator(),
            useFirstLineAsFieldNames: Boolean = false
        ): List<Map<String, String>> {

            // Store the content of the csv that will be returned
            val csvContent = mutableListOf<Map<String, String>>()

            // store the names that will be used in the csv fields
            var fieldNames: Array<String>? = null

            // iterate over each line
            var lineIndex = -1
            for (line in content.split(lineDelimiter)) {
                lineIndex++
                val fields = readCsvLine(line, fieldDelimiter)

                // ignore if no field is returned from the readCsvLine method
                if (fields.isEmpty()) {
                    continue
                }

                // if the first line is marked to be used as field names set it and go to the next line
                if (useFirstLineAsFieldNames && fieldNames == null) {
                    fieldNames = fields
                    continue
                }

                // if the number of field names is lesser than the number of fields throw an exception
                if (fieldNames != null && fieldNames.count() < fields.count()) {
                    throw Exception("The amount of field names used to identify the csv line fields is lesser than the number " +
                            "of fields. Field names: ${fieldNames}; Fields: $fields")
                }

                // iterate over each field
                val fieldsMap = mutableMapOf<String, String>()
                var fieldIndex = -1
                for (field in fields) {
                    fieldIndex++

                    // the fieldName will be the values stored in 'fieldNames' if defined, or the current index
                    val fieldName = if (fieldNames != null) fieldNames[fieldIndex] else fieldIndex.toString()

                    // add the current field into the fieldMap
                    fieldsMap[fieldName] = field
                }

                // include the map into the csvContent array
                csvContent.add(fieldsMap)
            }
            return csvContent
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
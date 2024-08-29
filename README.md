
## HashMatrix

`HashMatrix` é uma estrutura de dados em Kotlin projetada para gerar e comparar matrizes de hash baseadas em entradas de dados. Ela é útil para detectar diferenças entre grandes conjuntos de dados estruturados, oferecendo uma forma eficiente de verificar alterações em linhas específicas.

### Funcionalidades

- **Geração de Hashes**:
    - A estrutura permite calcular hashes para cada linha de uma matriz a partir de valores fornecidos.
    - Utiliza um algoritmo de hashing, como SHA-256, para garantir a integridade e a unicidade dos hashes gerados.

- **Comparação de Matrizes**:
    - Compara duas instâncias de `HashMatrix` e identifica linhas que foram adicionadas, removidas ou alteradas.
    - O resultado da comparação pode ser facilmente inspecionado para determinar as diferenças entre os conjuntos de dados.

### Como Funciona

1. **Construção da Matriz**:
    - Através da classe `HashMatrixBuilder`, você pode criar uma matriz de hash a partir de um conjunto de dados.
    - A classe permite a definição de um identificador único para cada linha, se desejado.

2. **Adição de Linhas e Valores**:
    - Linhas e valores são adicionados sequencialmente à matriz, com cada valor sendo digerido tanto no contexto da linha quanto no contexto da matriz como um todo.

3. **Comparação de Matrizes**:
    - Após a construção, as instâncias de `HashMatrix` podem ser comparadas usando o método `compare`, que retorna um objeto `HashMatrixComparison`.
    - Esse objeto fornece detalhes sobre as diferenças encontradas, indicando quais linhas foram adicionadas, removidas ou alteradas.

### Exemplo de Uso

```kotlin
val hm0 = HashMatrixBuilder.fromCsv(
    FileReader(File("path/to/file.csv")),
    { MessageDigest.getInstance("SHA256") },
    useColumnIndexAsIdentifier = 1
).build()

val hm1 = HashMatrixBuilder.fromCsv(
    FileReader(File("path/to/another_file.csv")),
    { MessageDigest.getInstance("SHA256") },
    useColumnIndexAsIdentifier = 1
).build()

val diff = hm0.compare(hm1)
if (diff.hasDifferences()) {
    println("Differences found:")
    for ((rowId, difference) in diff.rowsWithDifferences) {
        println("Row $rowId: $difference")
    }
}
```

### Desempenho

- Testado com entradas de 10.000 linhas e 15 colunas.
- Geração de matriz: ~148 ms por conjunto de dados.
- Comparação de matrizes: ~16 ms.

### Considerações Finais

`HashMatrix` é ideal para casos onde a integridade dos dados e a eficiência na detecção de alterações são essenciais. É especialmente útil em cenários de grandes volumes de dados, onde a verificação manual seria impraticável.

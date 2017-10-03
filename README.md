# sparql-to-tensor

This command-line tool allows you to export RDF data retrieved from a SPARQL endpoint to tensor slices in the [MatrixMarket](http://math.nist.gov/MatrixMarket/formats.html#MMformat) format. For each feature a matrix is written to a file named as `{feature local name}.mtx`. Matrices are serialized using the general coordinate format that is suitable for sparse matrices that RDF usually maps to. IRIs of entities in the matrices are written to the `headers.txt` file. Each IRI is written on a separate line. Line numbers are used as indices of entities in the matrices. The header can be thus used to translate the matrices to IRIs of RDF resources.

## Usage

Compile with [Leiningen](http://leiningen.org) and [lein-binplus](https://github.com/BrunoBonacci/lein-binplus):

```sh
git clone https://github.com/jindrichmynarz/sparql-to-tensor.git
cd sparql-to-tensor
lein bin
```

This produces an executable `target/sparql_to_tensor`. Observe its arguments:

```sh
target/sparql_to_csv --help
```

The arguments include the following:

* `-e`/`--endpoint`: URL of the queried SPARQL endpoint, such as `http://localhost:8890/sparql`
* `-q`/`--query`: A paged SPARQL query template in the [Mustache](https://mustache.github.io/mustache.5.html) syntax. The query template should use `{{limit}}` and `{{offset}}` to implement paging. This parameter may be repeated to use multiple query templates.
* `-o`/`--output`: Path to directory where the matrices and the header will be output. If the directory does not exist, it will be created.
* `-p`/`--page-size` (default = 10000): Page size indicating the maximum number of results retrieved in one request to SPARQL endpoint.
* `--symmetric`: A flag that for each relation between `A` and `B` creates the same relation between `B` and `A`. Turned off by default.

The query templates must generate SPARQL `SELECT` queries that project these variables:

* `?feature`: IRI of the feature (e.g., predicate), such as `<http://purl.org/dc/terms/subject>`.
* `?s`: IRI of the relation's subject
* `?o`: IRI of the relation's object
* `?weight` (optional, default = 1): Positive decimal number used as a weight of the relation.

For each feature a square matrix is created representing the adjacencies between `?s` and `?o`.

## Limitations

* Entities identified by blank nodes are not supported. Each entity must be identified with an IRI.
* Tested on matrices with hundreds of thousands entities. Larger matrices may be problematic to build, since they must fit into memory. 

## License

Copyright © 2017 Jindřich Mynarz

Distributed under the Eclipse Public License either version 1.0.

package parser

import (
	"os"

	"github.com/alecthomas/kingpin"
	"github.com/alecthomas/participle"
)

type Schema struct {
	Replication int64          ` "REPLICATION" "FACTOR" @Int ";" `
	Declaration []*Declaration `{ "CREATE" @@ }`
}

type Declaration struct {
	Namespace        *Namespace `  "NAMESPACE" @@ ";"`
	Table            *Table     `| "TABLE" @@ `
	TransactionTable *Table     `| "TRANSACTION" "TABLE" @@ `
}

type Replication struct {
	Replication int64 ` @Int `
}

type Namespace struct {
	Namespace string ` @Ident `
}

type Table struct {
	Namespace string    ` @Ident "." `
	Name      string    ` @Ident `
	Columns   []*Column ` "(" { @@ "," } ")"";" `
}

type Column struct {
	Name string ` @Ident `
	Type string ` @( "BIGINT" | "BLOB" | "BOOLEAN" | "DOUBLE" | "FLOAT" | "INT" | "TEXT") `
	Key  string ` [@( "PARTITIONKEY" | "CLUSTERINGKEY" )] `
}

func Parse(filePath *string) *Schema {
	parser, err := participle.Build(&Schema{})
	kingpin.FatalIfError(err, "")
	r, err := os.Open(*filePath)
	kingpin.FatalIfError(err, "")

	schema := &Schema{}
	err = parser.Parse(r, schema)
	kingpin.FatalIfError(err, "")
	return schema
}

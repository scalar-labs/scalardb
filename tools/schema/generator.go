package main

import (
	"./internal/generator"
	"./internal/parser"
	"fmt"
	"github.com/alecthomas/kingpin"
)

type SchemaFileGenerator interface {
	WriteFile(schema *parser.Schema, outputFile string) error
}

func main() {
	//Define the program flags and positional parameters
	var database = kingpin.Flag("database", "select the database of the generated schema file. Only 'cassandra' is supported for now.").Default("cassandra").String()
	var inputFile = kingpin.Arg("input_file", "path to the input schema file").Required().String()
	var outputFile = kingpin.Arg("output_file", "path to the generated output schema file").Required().String()
	//Parse input parameters
	kingpin.Parse()

	//Parse input file
	schema := parser.Parse(inputFile)
	//Generate output
	var schemaGenerator SchemaFileGenerator
	if *database == "cassandra" {
		schemaGenerator = generator.NewCassandraSchemaGenerator()
	} else {
		fmt.Printf("The schema generation is not supported for %s. Only 'cassandra' is supported for now.", *database)
		return
	}
	err := schemaGenerator.WriteFile(schema, *outputFile)
	kingpin.FatalIfError(err, "")
	fmt.Printf("The %s schema file %s has been sucessfully generated \n", *database, *outputFile)
}

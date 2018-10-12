package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/alecthomas/kingpin"
)

func main() {
	var database = kingpin.Flag("database", "select the database. Only 'cassandra' is supported for now").Default("cassandra").String()
	var inputFile = kingpin.Arg("input_file", "path to the input schema file").Required().String()
	kingpin.Parse()

	if *database == "cassandra" {
		outputFile := "./generated.cql"
		output, err := exec.Command("./generator", "--database", *database, *inputFile, outputFile).CombinedOutput()
		if err != nil {
			fmt.Printf("schema generation failed: %s", output)
			os.Exit(1)
		}
		fmt.Printf("%s", output)
		output, err = exec.Command("cqlsh", "-f", outputFile).CombinedOutput()
		if err != nil {
			fmt.Printf("schema loding failed: %s", output)
			os.Exit(1)
		}
	} else {
		fmt.Printf("schemaloader is not supported for %s. Only 'cassandra' is supported for now.", *database)
		os.Exit(1)
	}
	fmt.Printf("The schmea is loaded to %s sucessfully.\n", *database)
}

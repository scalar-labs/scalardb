package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/alecthomas/kingpin"
)

func main() {
	var database = kingpin.Flag("database", "select the database. Only 'cassandra' is supported for now.").Default("cassandra").String()
	var host = kingpin.Flag("host", "specify the host to connect.").Default("localhost").String()
	var inputFile = kingpin.Arg("input_file", "path to the input schema file").Required().String()
	kingpin.Parse()

	if *database == "cassandra" {
		outputFile := "./generated.cql"
		ex, err := os.Executable()
		if err != nil {
			panic(err)
		}
		output, err := exec.Command(filepath.Dir(ex)+"/generator", "--database", *database, *inputFile, outputFile).CombinedOutput()
		if err != nil {
			fmt.Printf("schema generation failed: %s\n", output)
			os.Exit(1)
		}
		fmt.Printf("%s", output)
		output, err = exec.Command("cqlsh", "-f", outputFile, *host).CombinedOutput()
		if err != nil {
			fmt.Printf("schema loading failed: %s\n", output)
			os.Exit(1)
		}
	} else {
		fmt.Printf("schemaloader is not supported for %s. Only 'cassandra' is supported for now.", *database)
		os.Exit(1)
	}
	fmt.Printf("The schema is loaded to %s successfully.\n", *database)
}

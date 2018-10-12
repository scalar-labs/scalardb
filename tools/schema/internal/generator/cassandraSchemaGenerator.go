package generator

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	//"scalar/indetail/ScalarDBGenerator/internal/app/parser"
	"../parser"
	"strings"

	"github.com/alecthomas/kingpin"
)

type CassandraSchemaGenerator struct {
}

func NewCassandraSchemaGenerator() *CassandraSchemaGenerator {
	return &CassandraSchemaGenerator{}
}

func (c *CassandraSchemaGenerator) WriteFile(schema *parser.Schema, outputFile string) error {
	builder := &strings.Builder{}
	isTransactional := false
	for _, d := range schema.Declaration {
		if d.Namespace != nil {
			c.addKeyspace(builder, d.Namespace, schema.Replication)
		} else if d.TransactionTable != nil {
			c.addTable(builder, d.TransactionTable, true)
			isTransactional = true
		} else {
			c.addTable(builder, d.Table, false)
		}
		builder.WriteString("\n")
	}
	if isTransactional {
		c.addCoordinator(builder, schema.Replication)
	}
	cleanedOutputFilePath := filepath.Clean(outputFile)
	outputDir := filepath.Dir(cleanedOutputFilePath)
	//Create the folder recursively if they do not exist
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		os.MkdirAll(outputDir, 0775)
	}
	err := ioutil.WriteFile(cleanedOutputFilePath, []byte(builder.String()), 0644)
	kingpin.FatalIfError(err, "")
	return nil
}

func (c *CassandraSchemaGenerator) addKeyspace(builder *strings.Builder, namespace *parser.Namespace, replication int64) {
	builder.WriteString(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION =", namespace.Namespace))
	builder.WriteString(fmt.Sprintf("{'class': 'SimpleStrategy', 'replication_factor': '%d'};\n", replication))
}

func (c *CassandraSchemaGenerator) addTable(builder *strings.Builder, table *parser.Table, useTransaction bool) {
	builder.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n", table.Namespace, table.Name))

	partitionKeys := []string{}
	clusteringKeys := []string{}
	for _, c := range table.Columns {
		builder.WriteString(fmt.Sprintf("\t%s %s,\n", c.Name, c.Type))
		if c.Key == "PARTITIONKEY" {
			partitionKeys = append(partitionKeys, c.Name)
		}
		if c.Key == "CLUSTERINGKEY" {
			clusteringKeys = append(clusteringKeys, c.Name)
		}
	}

	if useTransaction {
		c.addTransactionMetadataColumns(builder, table)
	}

	//throw error if no key is available
	builder.WriteString("\n\tPRIMARY KEY (")
	if len(partitionKeys) == 1 {
		builder.WriteString(fmt.Sprintf("%s", partitionKeys[0]))
	} else {
		builder.WriteString("(")
		for i, k := range partitionKeys {
			builder.WriteString(k)
			if i != (len(partitionKeys) - 1) {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(")")
	}
	if len(clusteringKeys) != 0 {
		builder.WriteString(", ")
		for i, k := range clusteringKeys {
			builder.WriteString(k)
			if i != (len(clusteringKeys) - 1) {
				builder.WriteString(", ")
			}
		}
	}
	builder.WriteString("),\n")
	builder.WriteString(");\n")
}

func (c *CassandraSchemaGenerator) addTransactionMetadataColumns(builder *strings.Builder, table *parser.Table) {
	builder.WriteString("\n")
	for _, c := range table.Columns {
		if c.Key != "PARTITIONKEY" && c.Key != "CLUSTERINGKEY" {
			builder.WriteString(fmt.Sprintf("\tbefore_%s %s,\n", c.Name, c.Type))
		}
	}
	builder.WriteString("\n")
	builder.WriteString("\ttx_id text,\n")
	builder.WriteString("\ttx_prepared_at bigint,\n")
	builder.WriteString("\ttx_committed_at bigint,\n")
	builder.WriteString("\ttx_state int,\n")
	builder.WriteString("\ttx_version int,\n")
	builder.WriteString("\n")
	builder.WriteString("\tbefore_tx_id text,\n")
	builder.WriteString("\tbefore_tx_prepared_at bigint,\n")
	builder.WriteString("\tbefore_tx_committed_at bigint,\n")
	builder.WriteString("\tbefore_tx_state int,\n")
	builder.WriteString("\tbefore_tx_version int,\n")
}

func (c *CassandraSchemaGenerator) addCoordinator(builder *strings.Builder, replication int64) {
	builder.WriteString("\n")
	builder.WriteString("DROP KEYSPACE IF EXISTS coordinator;\n")
	builder.WriteString(fmt.Sprintf("CREATE KEYSPACE coordinator WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '%d'};\n", replication))
	builder.WriteString("\n")
	builder.WriteString("CREATE TABLE coordinator.state (\n")
	builder.WriteString("\ttx_id text PRIMARY KEY,\n")
	builder.WriteString("\ttx_created_at bigint,\n")
	builder.WriteString("\ttx_state int\n")
	builder.WriteString(");")
}

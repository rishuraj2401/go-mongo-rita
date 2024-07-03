package cmd

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	kafkarita "go-mongo/kafka"
	"os"
	"os/exec"

	
)



func RunCommands(database string, config string, zeekPath string ,totalchunk string, chunk string){
    fmt.Println(chunk)
	cmdName := "sudo"
	args := []string{"rita", "--config", config, "import", "--rolling", "--numchunks", totalchunk, "--chunk" , chunk, zeekPath, database}

	// Create the command
	cmd := exec.Command(cmdName, args...)

	// Set the command's output to the standard output and error
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Run the command
	if err := cmd.Run(); err != nil {
		fmt.Printf("Error running command: %v\n", err)
	}

}

func ShowLongConnections(database string, kafkaAdd string){
	cmdName := "sudo"
	args := []string{"rita", "show-long-connections", database}

	// Create the command
	cmd := exec.Command(cmdName, args...)

	// Capture the output
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	// Run the command
	err := cmd.Run()
	if err != nil {
		fmt.Printf("Error running command: %s\n", err)
		return
	}

	// Parse the output as CSV
	csvReader := csv.NewReader(&stdout)
	records, err := csvReader.ReadAll()
	if err != nil {
		fmt.Printf("Error reading CSV: %s\n", err)
		return
	}

	// Check if there are any records
	if len(records) == 0 {
		fmt.Println("No records found")
		return
	}

	// Extract header
	header := records[0]

	// Convert CSV records to JSON
	for _, row := range records[1:] { // Skip the header row
		record := make(map[string]string)
		for i, field := range row {
			record[header[i]] = field
		}

		// Marshal individual JSON record
		jsonData, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			fmt.Printf("Error marshalling JSON: %s\n", err)
			continue
		}
        // fmt.Println(jsonData)
        kafkarita.PubToKafka("long_connections_rita", jsonData, kafkaAdd)
		
	}
}
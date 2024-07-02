package cmd

import (
	"fmt"
	"os"
	"os/exec"
)



func RunCommands(database string, config string, zeekPath string ,totalchunk string){

	cmdName := "sudo"
	args := []string{"rita", "--config", config, "import", "--rolling", "--numchunks", totalchunk, zeekPath, database}

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

// func ShowLongConnections(){
// 	cmdName := "sudo"
// 	args := []string{"rita", "show-beacons", database ,"-H"}

// 	// Create the command
// 	cmd := exec.Command(cmdName, args...)

// 	// Set the command's output to the standard output and error
// 	var out = os.Stdout
// 	cmd.Stderr = os.Stderr
// 	fmt.Println(out)

// 	// Run the command
// 	if err := cmd.Run(); err != nil {
// 		fmt.Printf("Error running command: %v\n", err)
// 	}

// }
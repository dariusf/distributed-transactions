package dtrans

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

var host string = "localhost"

func getHostName() string {
	out, err := exec.Command("hostname").Output()
	if err != nil {
		fmt.Println("Failed to obtain hostname")
		return ""
	}
	return strings.TrimSpace(string(out))
}

func getNodeId() string {
	return os.Getenv("NODE_ID")
}

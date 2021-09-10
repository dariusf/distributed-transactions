package dtrans

import (
	"bufio"
	"distributed-transactions/src/node"
	"fmt"
	"os"
	"regexp"
	"time"
)

var currentId int32 = 0

func Start(quitting chan bool) {
	go node.Start(quitting)

	reader := bufio.NewReader(os.Stdin)
	// fmt.Println("Starting transaction interface")

	const usage = `Operations:
  BEGIN
  SET <server>.<key> <value>
  GET <server>.<key>
  COMMIT
  ABORT`

	started := false
	var start time.Time

	// fmt.Println(usage)
	r, _ := regexp.Compile(`(BEGIN)|(SET) (.*)\.(.+) (.*)|(GET) (.*)\.(.*)|(COMMIT)|(ABORT)`)
	for {
		text, err := reader.ReadString('\n')
		// measure time from the first input
		if !started {
			started = true
			start = time.Now()
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading from stdin: %v\n", err)
			fmt.Printf("Total time taken: %d\n", time.Since(start).Nanoseconds())
			break
		}
		if r.MatchString(text) {
			res := r.FindStringSubmatch(text)
			for i := range res {
				if i > 0 && res[i] != "" {
					// changed to block
					runCommand(res, i)
					break
				}
			}
		} else {
			fmt.Fprintf(os.Stderr, "Error: Could not interpret input\n")
			// fmt.Println(usage)
		}
	}
}

func runCommand(cmds []string, i int) {
	if cmds[i] == "BEGIN" {
		err, tid := Begin()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot begin a transaction\n")
			return
		}
		currentId = tid
		// fmt.Printf("Beginning transaction %v\n", tid)
		fmt.Println("OK")

	} else if cmds[i] == "SET" {
		if currentId == 0 {
			fmt.Fprintf(os.Stderr, "Error: Must begin transaction before calling SET\n")
			return
		}
		err := Set(cmds[i+1], cmds[i+2], cmds[i+3], currentId)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not set: %v\n", err)
			currentId = 0
			return
		}
		// fmt.Printf("SETTING %v.%v = %v\n", cmds[i+1], cmds[i+2], cmds[i+3])
		fmt.Println("OK")

	} else if cmds[i] == "GET" {
		if currentId == 0 {
			fmt.Fprintf(os.Stderr, "Error: Must begin transaction before calling GET\n")
			return
		}
		res, err := Get(cmds[i+1], cmds[i+2], currentId)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not get: %v\n", err)
			if err.Error() != "No such object in server" {
				currentId = 0
			}
			return
		}
		if res != "" {
			fmt.Printf("%v.%v = %v\n", cmds[i+1], cmds[i+2], res)
		}

	} else if cmds[i] == "COMMIT" {
		if currentId == 0 {
			fmt.Fprintf(os.Stderr, "Error: Must begin transaction before calling COMMIT\n")
			return
		}
		err := Commit()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ABORT\n")
			currentId = 0
			return
		}
		fmt.Println("COMMIT OK")
		currentId = 0

	} else if cmds[i] == "ABORT" {
		if currentId == 0 {
			fmt.Fprintf(os.Stderr, "Error: Must begin transaction before calling ABORT\n")
			return
		}
		err := Abort()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error in abort: %v\n", err)
			return
		}

		fmt.Println("ABORT")
		currentId = 0

	} else {
		fmt.Fprintf(os.Stderr, "Error: Invalid command\n")
	}
}

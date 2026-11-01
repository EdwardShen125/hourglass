package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/EdwardShen125/hourglass"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	dsn := os.Getenv("HOURGLASS_DSN")
	if dsn == "" {
		dsn = "sqlite://hourglass.db"
	}

	switch cmd {
	case "stats":
		runStats(dsn)
	case "dlq":
		if len(args) < 1 {
			usage()
			os.Exit(2)
		}
		switch args[0] {
		case "ls":
			runDLQList(dsn, args[1:])
		case "replay":
			runDLQReplay(dsn, args[1:])
		default:
			fmt.Fprintf(os.Stderr, "hourglass: unknown dlq subcommand %q\n", args[0])
			usage()
			os.Exit(2)
		}
	default:
		fmt.Fprintf(os.Stderr, "hourglass: unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  hourglass stats                        show job/receipt counts by state
  hourglass dlq ls [--topic T] [-n N]    list dead-lettered jobs
  hourglass dlq replay <id...| --all>    requeue dead jobs to waiting

env:
  HOURGLASS_DSN  store dsn (default sqlite://hourglass.db)
`)
}

func openCLI(dsn string) *hourglass.Handle {
	h, err := hourglass.Open(dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hourglass: %v\n", err)
		os.Exit(1)
	}
	return h
}

func runStats(dsn string) {
	h := openCLI(dsn)
	defer h.Close()
	ctx := context.Background()
	st, err := h.Stats(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hourglass: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("jobs by state:\n")
	for _, line := range st.JobsByState {
		fmt.Printf("  %-10s %d\n", line.State, line.Count)
	}
	fmt.Printf("  %-10s %d\n", "total", st.TotalJobs)
	fmt.Printf("receipts: %d\n", st.TotalReceipts)
	fmt.Printf("oldest terminal kept: ")
	if st.OldestTerminal.IsZero() {
		fmt.Printf("(none)\n")
	} else {
		age := time.Since(st.OldestTerminal).Round(time.Second)
		fmt.Printf("%s (ago)\n", age)
	}
}

func runDLQList(dsn string, args []string) {
	topic := ""
	limit := 50
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--topic":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "hourglass: --topic needs a value")
				os.Exit(2)
			}
			i++
			topic = args[i]
		case "-n", "--limit":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "hourglass: -n needs a value")
				os.Exit(2)
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n <= 0 {
				fmt.Fprintln(os.Stderr, "hourglass: -n must be a positive integer")
				os.Exit(2)
			}
			limit = n
		default:
			fmt.Fprintf(os.Stderr, "hourglass: unknown flag %q\n", args[i])
			os.Exit(2)
		}
	}

	h := openCLI(dsn)
	defer h.Close()
	ctx := context.Background()
	jobs, err := h.ListDead(ctx, topic, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hourglass: %v\n", err)
		os.Exit(1)
	}
	if len(jobs) == 0 {
		fmt.Println("dlq: empty")
		return
	}
	for _, j := range jobs {
		fmt.Printf("%s  %s  attempts=%d  due=%s\n", j.ID, j.Topic, j.Attempts, j.Due.Format(time.RFC3339))
	}
}

func runDLQReplay(dsn string, args []string) {
	all := false
	var ids []string
	for _, a := range args {
		switch a {
		case "--all":
			all = true
		default:
			ids = append(ids, a)
		}
	}
	if !all && len(ids) == 0 {
		fmt.Fprintln(os.Stderr, "hourglass: dlq replay needs job ids or --all")
		os.Exit(2)
	}
	if all && len(ids) > 0 {
		fmt.Fprintln(os.Stderr, "hourglass: dlq replay: ids and --all are mutually exclusive")
		os.Exit(2)
	}

	h := openCLI(dsn)
	defer h.Close()
	ctx := context.Background()

	if all {
		n, err := h.ReplayAllDead(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "hourglass: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("replayed %d dead jobs\n", n)
		return
	}
	for _, id := range ids {
		if err := h.ReplayDead(ctx, id); err != nil {
			fmt.Fprintf(os.Stderr, "hourglass: %s: %v\n", id, err)
			os.Exit(1)
		}
		fmt.Printf("replayed %s\n", id)
	}
}

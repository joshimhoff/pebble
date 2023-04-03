package main

import (
	"fmt"
	cache2 "github.com/cockroachdb/pebble/cache"
	"log"
	"os"
	"sort"
	"unsafe"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/spf13/cobra"
)

var cacheConfig struct {
	replacementPolicy string
	size              int
}

var cacheCmd = &cobra.Command{
	Use:   "cache <trace>",
	Short: "",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run:   runCache,
}

func init() {
	cacheCmd.Flags().StringVar(
		&cacheConfig.replacementPolicy, "replacement-policy", "tinylfu",
		"")
	cacheCmd.Flags().IntVar(
		&cacheConfig.size, "size", 1024 * 1000,
		"")
}

type byStartUnixNano []objiotracing.Event

func (a byStartUnixNano) Len() int           { return len(a) }
func (a byStartUnixNano) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byStartUnixNano) Less(i, j int) bool { return a[i].StartUnixNano < a[j].StartUnixNano }

func runCache(cmd *cobra.Command, args []string) {
	// TODO(): Run tinylfu.
	// TODO(): Display results.
	// TODO(): Read trace incrementally.
	data, err := os.ReadFile(args[0])
	if err != nil {
		log.Fatal(err)
	}

	// TODO(): Delete.
/*	v := *(*objiotracing.Event)(unsafe.Pointer(&data[0]))
	fmt.Println(v.Size)
	fmt.Println(v.BlockType)

	trace2 := (*[2]objiotracing.Event)(unsafe.Pointer(&data[0]))[:]
	fmt.Println(trace2[0].Size)
	fmt.Println(trace2[0].BlockType)
	fmt.Println(trace2[1].Size)
	fmt.Println(trace2[1].BlockType)
*/

	numEvents := len(data) / int(unsafe.Sizeof(objiotracing.Event{}))
	p := (*objiotracing.Event)(unsafe.Pointer(&data[0]))
	trace := ([]objiotracing.Event)(unsafe.Slice(p, numEvents))
	// TODO(): Do these look correct?
	for _, e := range trace {
		fmt.Printf("%+v\n", e)
	}

	fmt.Println("SORTED BELOW ---")

	sort.Sort(byStartUnixNano(trace))
	for _, e := range trace {
		fmt.Printf("%+v\n", e)
	}

	fmt.Println("WIP BELOW ---")

	trace = append(trace, objiotracing.Event{Op: objiotracing.ReadOp, FileNum: 000005, Offset: 0})
	results := cache2.Simulate(trace, []cache2.Config{{
		Policy: cache2.ClockPro, Size: cacheConfig.size,
	}})

	fmt.Println("ANALYSIS BELOW ---")

	fmt.Printf("hits: %v\n", results[0].Hits)
	fmt.Printf("misses: %v\n", results[0].Misses)

	return
}

package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/dolthub/swiss"
)

const (
	FILE_NAME  = "./data/measurements.txt"
	BUFFER     = 16 * 1024 * 1024
	BUFFER_CH  = 10000
	CONCURRENT = 1000
)

func main() {
	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	start := time.Now()

	if err := run(); err != nil {
		log.Fatalf("Error %s", err)
	}

	end := time.Since(start)
	fmt.Printf("Time: %v\n", end)
}

func run() error {
	f, err := os.Open(FILE_NAME)
	if err != nil {
		return err
	}
	defer f.Close()

	ch := make(chan []byte, BUFFER_CH)

	go func() {
		for {
			data := make([]byte, BUFFER)

			_, err := f.Read(data)
			if errors.Is(err, io.EOF) {
				break
			}

			ch <- data
		}
		close(ch)
	}()

	out := make(chan Places)

	wg := sync.WaitGroup{}
	wg.Add(CONCURRENT)
	for i := 0; i < CONCURRENT; i++ {
		go func(index int) {
			defer wg.Done()
			for {
				val, ok := <-ch
				if !ok {
					break
				}

				places := Places{}

				scanner := bufio.NewScanner(bytes.NewReader(val))

				for scanner.Scan() {
					line := scanner.Bytes()

					parts := bytes.Split(line, []byte{';'})
					if len(parts) != 2 {
						continue
					}

					name := string(parts[0])
					temp, err := strconv.ParseFloat(string(parts[1]), 64)
					if err != nil {
						continue
					}

					val, ok := places[name]
					if ok {
						if val.Min >= temp {
							val.Min = temp
						}

						if val.Max <= temp {
							val.Max = temp
						}

						val.Sum += temp
						val.Count++
					} else {
						places[name] = &Summary{
							Min:   temp,
							Max:   temp,
							Sum:   temp,
							Count: 1,
						}
					}
				}

				out <- places
			}
		}(i)
	}

	result := swiss.NewMap[string, Summary](2048)

	go func() {
		for p := range out {
			for station, stationData := range p {

				val, ok := result.Get(station)
				if ok {
					if val.Min >= stationData.Min {
						val.Min = stationData.Min
					}

					if val.Max <= stationData.Max {
						val.Max = stationData.Max
					}

					val.Sum += stationData.Sum
					val.Count += stationData.Count
				} else {
					result.Put(station, Summary{
						Min:   stationData.Min,
						Max:   stationData.Max,
						Sum:   stationData.Sum,
						Count: stationData.Count,
					})
				}
			}
		}
	}()

	wg.Wait()
	close(out)

	result.Iter(func(k string, v Summary) (stop bool) {
		fmt.Printf("(%v) Place: %v, MIN: %v AVG: %v MAX: %v\n", v.Count, k, v.Min, v.Avg(), v.Max)
		return false
	})

	return nil
}

type Places map[string]*Summary

type Summary struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

func (s *Summary) Avg() float64 {
	return s.Sum / float64(s.Count)
}

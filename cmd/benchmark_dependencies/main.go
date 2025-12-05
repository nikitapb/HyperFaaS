package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	commonpb "github.com/3s-rg-codes/HyperFaaS/proto/common"
	leafpb "github.com/3s-rg-codes/HyperFaaS/proto/leaf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("expected 'yesDep' or 'noDep' subcommands")
		os.Exit(1)
	}

	var iterations int

	depCmd := flag.NewFlagSet("yesDep", flag.ExitOnError)
	noDepCmd := flag.NewFlagSet("noDep", flag.ExitOnError)

	depCmd.IntVar(&iterations, "iterations", 100, "number of iterations to go through")
	noDepCmd.IntVar(&iterations, "iterations", 100, "number of iterations to go through")

	var depSwitch bool

	switch os.Args[1] {
	case "yesDep":
		depSwitch = true
	case "noDep":
		depSwitch = false
	default:
		fmt.Println("expected 'yesDep' or 'noDep' subcommands")
		os.Exit(1)
	}
	if os.Args[1] == "yesDep" {
		depSwitch = true
	}

	TestFunction(iterations, depSwitch)
}

func TestFunction(iterations int, depSwitch bool) error {
	timeout := 10 * time.Second
	responseTimes := make([]float64, iterations)

	client, _, err := createLeafClient("localhost:50050")
	if err != nil {
		fmt.Println("Error creating leaf client, exiting!")
	}

	if depSwitch {
		funcData, err := CreateFunction(client, depSwitch)

		callReq := &commonpb.CallRequest{
			Data:       []byte(""),
			FunctionId: funcData.FunctionId,
		}

		if err != nil {
			return err
		}

		for i := 0; i < iterations; i++ {
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), timeout)

			callResp, err := client.ScheduleCall(ctx, callReq)

			cancel()

			elapsed := time.Since(start)

			if err != nil {
				fmt.Printf("[%v] Error scheduling call: %v\n", i+1, err)
				return err
			} else if callResp == nil {
				fmt.Printf("[%v] WARNING: Response is nil!\n", i+1)
			} else {
				fmt.Printf("[%v] Successfully called function!\n", i+1)
			}
			responseTimes[i] = float64(elapsed.Milliseconds())
		}
	} else {

		for i := 0; i < iterations; i++ {
			funcData, err := CreateFunction(client, depSwitch)

			callReq := &commonpb.CallRequest{
				Data:       []byte(""),
				FunctionId: funcData.FunctionId,
			}

			if err != nil {
				return err
			}

			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), timeout)

			callResp, err := client.ScheduleCall(ctx, callReq)

			cancel()

			elapsed := time.Since(start)

			if err != nil {
				fmt.Printf("[%v] Error scheduling call: %v\n", i+1, err)
				return err
			} else if callResp == nil {
				fmt.Printf("[%v] WARNING: Response is nil!\n", i+1)
			} else {
				fmt.Printf("[%v] Successfully called function!\n", i+1)
			}
			responseTimes[i] = float64(elapsed.Milliseconds())
		}
	}

	var filename string

	now := time.Now()
	formatted := now.Format("2006-01-02 15:04:05")

	if depSwitch {
		filename = fmt.Sprintf("%v-withDependencyFeature.csv", formatted)
	} else {
		filename = fmt.Sprintf("%v-withoutDependencyFeature.csv", formatted)
	}

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := make([]string, len(responseTimes))
	for i, v := range responseTimes {
		record[i] = strconv.FormatFloat(v, 'f', -1, 64)
	}

	if err := writer.Write(record); err != nil {
		fmt.Println("Error writing CSV:", err)
		return nil
	}

	return nil
}

func CreateFunction(client leafpb.LeafClient, depFlag bool) (*leafpb.CreateFunctionResponse, error) {

	var cpuPeriod int64 = 100000
	var cpuQuota int64 = 50000
	var memory int64 = 1024 * 1024 * 256
	timeout := 10 * time.Second

	cpuConfig := &commonpb.CPUConfig{
		Period: cpuPeriod,
		Quota:  cpuQuota,
	}

	config := &commonpb.Config{
		Memory: memory,
		Cpu:    cpuConfig,
	}
	var imageTag string
	var dependencies [2]string
	if depFlag {
		imageTag = "hyperfaas-hellodependencies:latest"
		dependencies = [2]string{"hyperfaas-hello:latest", "hyperfaas-echo:latest"}
		config.FunctionDependencies = dependencies[:]
	} else {
		imageTag = "hyperfaas-byebyedependencies:latest"
	}

	funcReq := &leafpb.CreateFunctionRequest{
		Image: &commonpb.Image{
			Tag: imageTag,
		},
		Config: config,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	funcResp, err := client.CreateFunction(ctx, funcReq)
	if err != nil {
		fmt.Printf("Error creating function: %v\n", err)
		return nil, err
	}

	if depFlag {
		fmt.Printf("Created function from imageTag: '%v'\nFunction id: %v\nPassed dependencies:%v\n", imageTag, funcResp.FunctionId, dependencies)
	} else {
		fmt.Printf("Created function from imageTag: '%v'\nFunction id: %v\n", imageTag, funcResp.FunctionId)
	}
	return funcResp, nil
}

func createLeafClient(address string) (leafpb.LeafClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return leafpb.NewLeafClient(conn), conn, nil
}

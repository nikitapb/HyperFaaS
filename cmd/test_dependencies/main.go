package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	commonpb "github.com/3s-rg-codes/HyperFaaS/proto/common"
	leafpb "github.com/3s-rg-codes/HyperFaaS/proto/leaf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("expected 'create' or 'test' subcommands")
		os.Exit(1)
	}

	createCmd := flag.NewFlagSet("create", flag.ExitOnError)
	testCmd := flag.NewFlagSet("test", flag.ExitOnError)

	depFlag := createCmd.Bool("dep", true, "use dependencies or not")
	functionID := testCmd.String("fID", "", "function ID")

	client, _, err := createLeafClient("localhost:50050")
	if err != nil {
		fmt.Println("Error creating leaf client, exiting!")
	}

	// Switch based on subcommand
	switch os.Args[1] {
	case "create":
		_ = createCmd.Parse(os.Args[2:])

		CreateFunction(client, *depFlag)

	case "test":
		_ = testCmd.Parse(os.Args[2:])

		TestFunction(client, *functionID)

	default:
		fmt.Println("expected 'create' or 'test' subcommands")
		os.Exit(1)
	}

}

func CreateFunction(client leafpb.LeafClient, depFlag bool) (string, error) {

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
		return "", err
	}

	if depFlag {
		fmt.Printf("Created function from imageTag: '%v'\nFunction id: %v\nPassed dependencies:%v\n", imageTag, funcResp.FunctionId, dependencies)
	} else {
		fmt.Printf("Created function from imageTag: '%v'\nFunction id: %v\n", imageTag, funcResp.FunctionId)
	}
	return funcResp.FunctionId, nil
}

func TestFunction(client leafpb.LeafClient, fID string) (*commonpb.CallResponse, error) {
	print(fID)
	timeout := 10 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	callReq := &commonpb.CallRequest{
		Data:       []byte(""),
		FunctionId: fID,
	}

	callResp, err := client.ScheduleCall(ctx, callReq)

	if err != nil {
		fmt.Printf("TestFunction(): Error scheduling call: %v\n", err)
		return nil, err
	} else if callResp == nil {
		fmt.Print("No error, but response is nil!\n")
	} else {
		fmt.Printf("Response: %v\n", string(callResp.Data))
	}

	return callResp, nil
}

func createLeafClient(address string) (leafpb.LeafClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return leafpb.NewLeafClient(conn), conn, nil
}

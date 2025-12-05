package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/3s-rg-codes/HyperFaaS/pkg/worker/functionRuntimeInterface"
	common "github.com/3s-rg-codes/HyperFaaS/proto/common"
	leafpb "github.com/3s-rg-codes/HyperFaaS/proto/leaf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	f := functionRuntimeInterface.New(10)

	f.Ready(handler)
}

func handler(ctx context.Context, in *common.CallRequest) (*common.CallResponse, error) {

	dependencies := [2]string{"hyperfaas-hello:latest", "hyperfaas-echo:latest"}

	//first call helloworld
	helloresponse, err := dependencyCaller(dependencies[0], []byte{})

	if err != nil {
		return nil, err
	}

	mapAsByte, err := json.Marshal(dependencies)

	if err != nil {
		return nil, err
	}

	//then call echo
	echoresponse, err := dependencyCaller(dependencies[1], mapAsByte)

	response := string(helloresponse.Data) + string(echoresponse.Data)

	resp := &common.CallResponse{
		Data:  []byte(response),
		Error: nil,
	}

	return resp, nil
}

func dependencyCaller(imageTag string, data []byte) (*common.CallResponse, error) {
	client, _, err := createLeafClient("127.0.0.1:50050")
	if err != nil {
		return nil, fmt.Errorf("failed establishing client connection! %w\n", err)
	}

	var cpuPeriod int64 = 100000
	var cpuQuota int64 = 50000
	var memory int64 = 1024 * 1024 * 256
	timeout := 10 * time.Second

	cpuConfig := &common.CPUConfig{
		Period: cpuPeriod,
		Quota:  cpuQuota,
	}

	config := &common.Config{
		Memory: memory,
		Cpu:    cpuConfig,
	}

	createReq := &leafpb.CreateFunctionRequest{
		Image: &common.Image{
			Tag: imageTag,
		},
		Config: config,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	createResp, err := client.CreateFunction(ctx, createReq)

	if err != nil {
		fmt.Printf("function: Error creating dependency-function: %v\n", err)
		cancel()
		return nil, err
	}

	scheduleCallReq := &common.CallRequest{
		FunctionId: createResp.FunctionId,
		Data:       data,
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	scheduleCallResponse, err := client.ScheduleCall(ctx2, scheduleCallReq)

	if err != nil {
		fmt.Printf("function: Error scheduling call: %v\n", err)
		return nil, err
	}
	if scheduleCallResponse.Error != nil {
		fmt.Printf("function: Internal error scheduling call: %v\n", scheduleCallResponse.Error)
		return nil, errors.New(scheduleCallResponse.Error.Message)
	}

	return scheduleCallResponse, nil
}

func createLeafClient(address string) (leafpb.LeafClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return leafpb.NewLeafClient(conn), conn, nil
}

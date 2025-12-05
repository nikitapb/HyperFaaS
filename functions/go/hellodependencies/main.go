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

	dependencyMap := in.DependencyMap

	//first call helloworld
	helloresponse, err := dependencyCaller(dependencyMap["hyperfaas-hello:latest"], []byte{})

	if err != nil {
		return nil, err
	}

	mapAsByte, err := json.Marshal(dependencyMap)

	if err != nil {
		return nil, err
	}

	//then call echo
	echoresponse, err := dependencyCaller(dependencyMap["hyperfaas-echo:latest"], mapAsByte)

	response := string(helloresponse.Data) + string(echoresponse.Data)

	resp := &common.CallResponse{
		Data:  []byte(response),
		Error: nil,
	}

	return resp, nil
}

func dependencyCaller(functionID string, data []byte) (*common.CallResponse, error) {

	client, _, err := createLeafClient("127.0.0.1:50050")
	if err != nil {
		return nil, fmt.Errorf("failed establishing client connection! %w\n", err)
	}
	scheduleCallReq := &common.CallRequest{
		FunctionId: functionID,
		Data:       data,
	}

	ctx, cancel := context.WithTimeout(context.Background(), (time.Second * 15))
	defer cancel()

	scheduleCallResponse, err := client.ScheduleCall(ctx, scheduleCallReq)

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

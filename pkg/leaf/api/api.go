package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	kv "github.com/3s-rg-codes/HyperFaaS/pkg/keyValueStore"
	"github.com/3s-rg-codes/HyperFaaS/pkg/leaf/config"
	"github.com/3s-rg-codes/HyperFaaS/pkg/leaf/state"
	"github.com/3s-rg-codes/HyperFaaS/proto/common"
	commonpb "github.com/3s-rg-codes/HyperFaaS/proto/common"
	"github.com/3s-rg-codes/HyperFaaS/proto/leaf"
	workerPB "github.com/3s-rg-codes/HyperFaaS/proto/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Here we could have a cache of functions and if they are scale 0 or 1.

type LeafServer struct {
	leaf.UnimplementedLeafServer
	state                    *state.SmallState
	leafConfig               config.LeafConfig
	workerIds                []state.WorkerID
	functionMetricChans      map[state.FunctionID]chan bool
	functionMetricChansMutex sync.RWMutex
	database                 kv.FunctionMetadataStore
	functionIdCache          map[string]kv.FunctionData
	workerClients            workerClients
	logger                   *slog.Logger
	dependencyCache          map[string]dependencyData
}

type workerClients struct {
	mu      sync.RWMutex
	clients map[state.WorkerID]workerClient
}

type workerClient struct {
	conn   *grpc.ClientConn
	client workerPB.WorkerClient
}

type dependencyData struct {
	functionID string
	instanceID string
	ip         string
}

type dependencyUpdate struct {
	dependency string
	address    string
}

type DependencyMap struct {
	mu sync.RWMutex
	m  map[string]string
}

func NewDependencyMap(defaults map[string]string) *DependencyMap {
	return &DependencyMap{
		m: defaults,
	}
}

func (s *DependencyMap) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok
}

func (s *DependencyMap) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
}

// CreateFunction should only create the function, e.g. save its Config and image tag in local cache
func (s *LeafServer) CreateFunction(ctx context.Context, req *leaf.CreateFunctionRequest) (*leaf.CreateFunctionResponse, error) {

	functionID, err := s.database.Put(req.Image, req.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to store function in database: %w", err)
	}

	s.functionIdCache[functionID] = kv.FunctionData{
		Config: req.Config, //Also needed here for scheduling decisions
		Image:  req.Image,
	}

	var dependencies []string //slice of imageTags, that are the function's dependencies
	dependencyMap := s.StartDependencies(ctx, functionID, req.Image.Tag, dependencies)

	//next, start function itself
	startResp, err := s.startInstance(ctx, s.workerIds[0], state.FunctionID(functionID), dependencyMap) //also pass dependencyMap

	if err != nil {
		return nil, err
	}
	s.dependencyCache[req.Image.Tag] = dependencyData{
		instanceID: string(startResp.InstanceId),
		functionID: functionID,
		ip:         startResp.InstanceIp,
	}

	s.functionMetricChansMutex.Lock()
	s.functionMetricChans[state.FunctionID(functionID)] = make(chan bool, 10000)
	s.functionMetricChansMutex.Unlock()

	s.state.AddFunction(state.FunctionID(functionID),
		s.functionMetricChans[state.FunctionID(functionID)],
		func(ctx context.Context, functionID state.FunctionID, workerID state.WorkerID) error {
			_, err := s.startInstance(ctx, workerID, functionID, nil)
			if err != nil {
				return err
			}
			return nil
		})

	return &leaf.CreateFunctionResponse{
		FunctionId: functionID,
	}, nil

}

func (s *LeafServer) StartDependencies(ctx context.Context, functionID string, imageTag string, dependencies []string) *DependencyMap {

	//first check for dependencies
	dependencyAddresses := NewDependencyMap(make(map[string]string)) //map of imageTag -> address to pass to function

	updates := make(chan dependencyUpdate)

	go func() {
		for u := range updates {
			dependencyAddresses.Set(u.dependency, u.address)
		}
	}()

	for _, dependency := range dependencies {
		v, ok := s.dependencyCache[dependency] // check if dependency already instantiated

		if ok { // if yes, add
			dependencyAddresses.Set(dependency, v.ip)
		} else { // if no, create new and then add
			var cpuPeriod int64
			var cpuQuota int64
			var memory int64

			cpuConfig := &commonpb.CPUConfig{
				Period: cpuPeriod,
				Quota:  cpuQuota,
			}

			config := &commonpb.Config{
				Memory: memory,
				Cpu:    cpuConfig,
			}

			image := &commonpb.Image{
				Tag: dependency,
			}

			createReq := &leaf.CreateFunctionRequest{
				Image:  image,
				Config: config,
			}

			go func() {
				resp, _ := s.CreateFunction(ctx, createReq)
				newAddr := resp.FunctionId
				updates <- dependencyUpdate{
					dependency: dependency,
					address:    newAddr,
				}
			}()
		}
	}

	return dependencyAddresses

}

func (s *LeafServer) ScheduleCall(ctx context.Context, req *commonpb.CallRequest) (*commonpb.CallResponse, error) {
	autoscaler, ok := s.state.GetAutoscaler(state.FunctionID(req.FunctionId))
	if !ok {
		return nil, status.Errorf(codes.NotFound, "function id not found")
	}
	if autoscaler.IsScaledDown() {
		err := autoscaler.ForceScaleUp(ctx)
		if err != nil {
			if errors.As(err, &state.TooManyStartingInstancesError{}) {
				time.Sleep(s.leafConfig.PanicBackoff)
				s.leafConfig.PanicBackoff = s.leafConfig.PanicBackoff + s.leafConfig.PanicBackoffIncrease
				if s.leafConfig.PanicBackoff > s.leafConfig.PanicMaxBackoff {
					s.leafConfig.PanicBackoff = s.leafConfig.PanicMaxBackoff
				}
				return s.ScheduleCall(ctx, req)
			}
			if errors.As(err, &state.ScaleUpFailedError{}) {
				//TODO improve error message with better info.
				return nil, status.Errorf(codes.ResourceExhausted, "failed to scale up function")
			}
		}
	}
	if autoscaler.IsPanicMode() {
		time.Sleep(s.leafConfig.PanicBackoff)
		s.leafConfig.PanicBackoff = s.leafConfig.PanicBackoff + s.leafConfig.PanicBackoffIncrease
		if s.leafConfig.PanicBackoff > s.leafConfig.PanicMaxBackoff {
			s.leafConfig.PanicBackoff = s.leafConfig.PanicMaxBackoff
		}
		return s.ScheduleCall(ctx, req)
	}

	// TODO: pick a better way to pick a worker.
	randWorker := s.workerIds[rand.Intn(len(s.workerIds))]

	s.functionMetricChansMutex.RLock()
	metricChan := s.functionMetricChans[state.FunctionID(req.FunctionId)]
	s.functionMetricChansMutex.RUnlock()
	metricChan <- true
	// Note: we send function id as instance id because I havent updated the proto yet. But the call instance endpoint is now call function. worker handles the instance id.
	resp, err := s.callWorker(ctx, randWorker, state.FunctionID(req.FunctionId), state.InstanceID(req.FunctionId), req)
	if err != nil {
		return nil, err
	}
	metricChan <- false

	return resp, nil
}

func NewLeafServer(
	leafConfig config.LeafConfig,
	httpClient kv.FunctionMetadataStore,
	workerIds []state.WorkerID,
	logger *slog.Logger,
) *LeafServer {
	ls := LeafServer{
		database:            httpClient,
		functionIdCache:     make(map[string]kv.FunctionData),
		functionMetricChans: make(map[state.FunctionID]chan bool),
		workerClients:       workerClients{clients: make(map[state.WorkerID]workerClient)},
		workerIds:           workerIds,
		state:               state.NewSmallState(workerIds, logger),
		logger:              logger,
		leafConfig:          leafConfig,
		dependencyCache:     make(map[string]dependencyData),
	}
	ls.state.RunReconciler(context.Background())
	return &ls
}

func (s *LeafServer) callWorker(ctx context.Context, workerID state.WorkerID, functionID state.FunctionID, instanceID state.InstanceID, req *commonpb.CallRequest) (*common.CallResponse, error) {
	client, err := s.getOrCreateWorkerClient(workerID)
	if err != nil {
		return nil, err
	}

	var resp *commonpb.CallResponse
	callReq := &commonpb.CallRequest{
		FunctionId: string(functionID),
		Data:       req.Data,
	}

	resp, err = client.Call(ctx, callReq)
	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Unavailable {
			return nil, &WorkerDownError{WorkerID: workerID, err: err}
		}
		return nil, err
	}

	return resp, nil
}

func (s *LeafServer) startInstance(ctx context.Context, workerID state.WorkerID, functionId state.FunctionID, dependencyMap *DependencyMap) (*workerPB.StartResponse, error) {
	client, err := s.getOrCreateWorkerClient(workerID)
	if err != nil {
		return nil, err
	}
	resp, err := client.Start(ctx, &workerPB.StartRequest{FunctionId: string(functionId)})
	if err != nil {
		return nil, err
	}

	return resp, nil //we should use proto here, too
	//return resp
}

func (s *LeafServer) getOrCreateWorkerClient(workerID state.WorkerID) (workerPB.WorkerClient, error) {
	s.workerClients.mu.RLock()
	client, ok := s.workerClients.clients[workerID]
	s.workerClients.mu.RUnlock()
	if !ok {

		c, err := grpc.NewClient(string(workerID), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC client: %w", err)
		}
		cl := workerPB.NewWorkerClient(c)
		s.workerClients.mu.Lock()
		s.workerClients.clients[workerID] = workerClient{
			conn:   c,
			client: cl,
		}
		s.workerClients.mu.Unlock()
		return cl, nil
	}
	return client.client, nil
}

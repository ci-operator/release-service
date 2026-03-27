package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	TracerName      = "release-service"
	EnvOTLPEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"
)

// Span attribute keys.
var (
	DeliveryStageKey       = attribute.Key("delivery.stage")
	DeliveryApplicationKey = attribute.Key("delivery.application")
	DeliveryComponentKey   = attribute.Key("delivery.component")
	DeliverySuccessKey     = attribute.Key("delivery.success")
	DeliveryReasonKey      = attribute.Key("delivery.reason")

	TektonPipelineRunNameKey = attribute.Key("tekton.pipelinerun.name")
	TektonPipelineRunUIDKey  = attribute.Key("tekton.pipelinerun.uid")
)

var setupLog = ctrl.Log.WithName("tracing")

type TracerProvider struct {
	provider trace.TracerProvider
	shutdown func(context.Context) error
}

func New(ctx context.Context) (*TracerProvider, error) {
	if os.Getenv(EnvOTLPEndpoint) == "" {
		setupLog.Info("OTLP endpoint not configured, using noop tracer provider")
		return &TracerProvider{
			provider: trace.NewNoopTracerProvider(),
			shutdown: func(context.Context) error { return nil },
		}, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpointURL(os.Getenv(EnvOTLPEndpoint)),
	)
	if err != nil {
		setupLog.Error(err, "failed to create OTLP exporter, using noop tracer provider")
		return &TracerProvider{
			provider: trace.NewNoopTracerProvider(),
			shutdown: func(context.Context) error { return nil },
		}, nil
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(TracerName),
		),
	)
	if err != nil {
		setupLog.Error(err, "failed to create resource")
		res = resource.Default()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	setupLog.Info("tracing initialized", "endpoint", os.Getenv(EnvOTLPEndpoint))

	return &TracerProvider{
		provider: tp,
		shutdown: tp.Shutdown,
	}, nil
}

func (tp *TracerProvider) Shutdown(ctx context.Context) error {
	if tp.shutdown != nil {
		return tp.shutdown(ctx)
	}
	return nil
}

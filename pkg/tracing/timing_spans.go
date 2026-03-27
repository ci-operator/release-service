package tracing

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"knative.dev/pkg/apis"

	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"

	"github.com/konflux-ci/release-service/metadata"
	corev1 "k8s.io/api/core/v1"
)

const (
	stageRelease = "release"
)

// CtxFromSpanContext creates a context with the trace as parent.
func CtxFromSpanContext(jsonCarrier string) (context.Context, bool) {
	if jsonCarrier == "" {
		return context.Background(), false
	}
	var carrier map[string]string
	if err := json.Unmarshal([]byte(jsonCarrier), &carrier); err != nil {
		return context.Background(), false
	}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(carrier))
	sc := trace.SpanContextFromContext(ctx)
	return ctx, sc.IsValid()
}

func setCommonAttributes(span trace.Span, pr *tektonv1.PipelineRun) {
	span.SetAttributes(
		semconv.K8SNamespaceName(pr.Namespace),
		TektonPipelineRunNameKey.String(pr.Name),
		TektonPipelineRunUIDKey.String(string(pr.UID)),
		DeliveryStageKey.String(stageRelease),
	)
	if app, ok := pr.Labels[metadata.ApplicationNameLabel]; ok && app != "" {
		span.SetAttributes(DeliveryApplicationKey.String(app))
	}
}

func setOutcomeAttributes(span trace.Span, pr *tektonv1.PipelineRun) {
	condition := pr.Status.GetCondition(apis.ConditionSucceeded)
	success := false
	reason := ""
	if condition != nil {
		success = condition.Status == corev1.ConditionTrue
		reason = condition.Reason
	}
	span.SetAttributes(
		DeliverySuccessKey.Bool(success),
		DeliveryReasonKey.String(reason),
	)
}

// EmitWaitDuration emits a waitDuration span for a PipelineRun.
func EmitWaitDuration(ctx context.Context, pr *tektonv1.PipelineRun) {
	if pr.Status.StartTime == nil {
		return
	}
	start := pr.CreationTimestamp.Time
	end := pr.Status.StartTime.Time
	if end.Before(start) {
		return
	}

	tr := otel.Tracer(TracerName)
	_, span := tr.Start(ctx, "waitDuration",
		trace.WithTimestamp(start),
	)

	setCommonAttributes(span, pr)
	span.End(trace.WithTimestamp(end))
}

// EmitExecuteDuration emits an executeDuration span for a PipelineRun.
func EmitExecuteDuration(ctx context.Context, pr *tektonv1.PipelineRun) {
	if pr.Status.StartTime == nil || pr.Status.CompletionTime == nil {
		return
	}
	start := pr.Status.StartTime.Time
	end := pr.Status.CompletionTime.Time
	if end.Before(start) {
		return
	}

	tr := otel.Tracer(TracerName)
	_, span := tr.Start(ctx, "executeDuration",
		trace.WithTimestamp(start),
	)

	setCommonAttributes(span, pr)
	setOutcomeAttributes(span, pr)
	span.End(trace.WithTimestamp(end))
}

// EmitTimingSpans emits both waitDuration and executeDuration spans for a completed release PipelineRun.
func EmitTimingSpans(pr *tektonv1.PipelineRun, spanContext string) bool {
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		return false
	}

	parentCtx, ok := CtxFromSpanContext(spanContext)
	if !ok {
		parentCtx = context.Background()
	}

	if pr.Status.StartTime == nil {
		return false
	}

	EmitWaitDuration(parentCtx, pr)

	if pr.Status.CompletionTime != nil {
		EmitExecuteDuration(parentCtx, pr)
	}

	return true
}

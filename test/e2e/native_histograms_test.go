// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestQueryNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-query")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "ha1", e2ethanos.DefaultPromConfig("prom-ha", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "ha2", e2ethanos.DefaultPromConfig("prom-ha", 1, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL1 := "http://" + prom1.Endpoint("http") + "/api/v1/write"
	rawRemoteWriteURL2 := "http://" + prom2.Endpoint("http") + "/api/v1/write"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	histograms := generateHistograms(1)
	now := time.Now()

	_, err = writeHistograms(ctx, now, histograms, rawRemoteWriteURL1)
	testutil.Ok(t, err)
	_, err = writeHistograms(ctx, now, histograms, rawRemoteWriteURL2)
	testutil.Ok(t, err)

	ts := func() time.Time { return now }

	// Make sure we can query histogram from both Prometheus instances.
	queryAndAssert(t, ctx, prom1.Endpoint("http"), func() string { return "fake_histogram" }, ts, promclient.QueryOptions{}, expectedHistogramModelVector(histograms[0], nil))
	queryAndAssert(t, ctx, prom2.Endpoint("http"), func() string { return "fake_histogram" }, ts, promclient.QueryOptions{}, expectedHistogramModelVector(histograms[0], nil))

	// Query deduplicated histogram from Thanos Querier.
	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return "fake_histogram" }, ts, promclient.QueryOptions{Deduplicate: true}, expectedHistogramModelVector(histograms[0], map[string]string{
		"prometheus": "prom-ha",
	}))

	// Query histogram using histogram_count function and deduplication from Thanos Querier.
	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return "histogram_count(fake_histogram)" }, ts, promclient.QueryOptions{Deduplicate: true}, model.Vector{
		&model.Sample{
			Value: 5,
			Metric: model.Metric{
				"foo":        "bar",
				"prometheus": "prom-ha",
			},
		},
	})

	// Query histogram using group function to test pushdown.
	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return "group(fake_histogram)" }, time.Now, promclient.QueryOptions{Deduplicate: true}, model.Vector{
		&model.Sample{
			Value:  1,
			Metric: model.Metric{},
		},
	})
}

func TestWriteNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-write")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ingestor0 := e2ethanos.NewReceiveBuilder(e, "ingestor0").WithIngestionEnabled().WithNativeHistograms().Init()
	ingestor1 := e2ethanos.NewReceiveBuilder(e, "ingestor1").WithIngestionEnabled().WithNativeHistograms().Init()

	h := receive.HashringConfig{
		Endpoints: []string{
			ingestor0.InternalEndpoint("grpc"),
			ingestor1.InternalEndpoint("grpc"),
		},
	}

	router0 := e2ethanos.NewReceiveBuilder(e, "router0").WithRouting(2, h).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(ingestor0, ingestor1, router0))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", ingestor0.InternalEndpoint("grpc"), ingestor1.InternalEndpoint("grpc")).WithReplicaLabels("receive").Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL := "http://" + router0.Endpoint("remote-write") + "/api/v1/receive"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	histograms := generateHistograms(1)
	now := time.Now()
	_, err = writeHistograms(ctx, now, histograms, rawRemoteWriteURL)
	testutil.Ok(t, err)

	ts := func() time.Time { return now }

	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return "fake_histogram" }, ts, promclient.QueryOptions{Deduplicate: true}, expectedHistogramModelVector(histograms[0], map[string]string{
		"tenant_id": "default-tenant",
	}))
}

func TestQueryFrontendNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-qfe")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "ha1", e2ethanos.DefaultPromConfig("prom-ha", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "ha2", e2ethanos.DefaultPromConfig("prom-ha", 1, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.INMEMORY,
		Config: queryfrontend.InMemoryResponseCacheConfig{
			MaxSizeItems: 1000,
			Validity:     time.Hour,
		},
	}

	qf := e2ethanos.NewQueryFrontend(e, "query-frontend", "http://"+querier.InternalEndpoint("http"), queryfrontend.Config{}, inMemoryCacheConfig)
	testutil.Ok(t, e2e.StartAndWaitReady(qf))

	rawRemoteWriteURL1 := "http://" + prom1.Endpoint("http") + "/api/v1/write"
	rawRemoteWriteURL2 := "http://" + prom2.Endpoint("http") + "/api/v1/write"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	histograms := generateHistograms(4)
	now := time.Now()
	_, err = writeHistograms(ctx, now, histograms, rawRemoteWriteURL1)
	testutil.Ok(t, err)
	startTime, err := writeHistograms(ctx, now, histograms, rawRemoteWriteURL2)
	testutil.Ok(t, err)

	ts := func() time.Time { return now }

	// instant query
	queryAndAssert(t, ctx, qf.Endpoint("http"), func() string { return "fake_histogram" }, ts, promclient.QueryOptions{Deduplicate: true}, expectedHistogramModelVector(histograms[len(histograms)-1], map[string]string{
		"prometheus": "prom-ha",
	}))

	expectedRes := expectedHistogramModelMatrix(histograms, startTime, map[string]string{
		"prometheus": "prom-ha",
	})

	rangeQuery(t, ctx, qf.Endpoint("http"), func() string { return "fake_histogram" }, startTime.UnixMilli(),
		now.UnixMilli(),
		30, // Taken from UI.
		promclient.QueryOptions{
			Deduplicate: true,
		}, func(res model.Matrix) error {
			if !reflect.DeepEqual(res, expectedRes) {
				return fmt.Errorf("unexpected results (got %v but expected %v)", res, expectedRes)
			}
			return nil
		})
}

func generateHistograms(n int) []*histogram.Histogram {
	return tsdb.GenerateTestHistograms(n)
}

func writeHistograms(ctx context.Context, now time.Time, histograms []*histogram.Histogram, rawRemoteWriteURL string) (time.Time, error) {
	startTime := now.Add(time.Duration(len(histograms)-1) * -30 * time.Second).Truncate(30 * time.Second)
	prompbHistograms := make([]prompb.Histogram, 0, len(histograms))

	for i, h := range histograms {
		ts := startTime.Add(time.Duration(i) * 30 * time.Second).UnixMilli()
		prompbHistograms = append(prompbHistograms, remote.HistogramToHistogramProto(ts, h))
	}

	timeSeriespb := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "fake_histogram"},
			{Name: "foo", Value: "bar"},
		},
		Histograms: prompbHistograms,
	}

	return startTime, storeWriteRequest(ctx, rawRemoteWriteURL, &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{timeSeriespb},
	})
}

func expectedHistogramModelVector(histogram *histogram.Histogram, externalLabels map[string]string) model.Vector {
	metrics := model.Metric{
		"__name__": "fake_histogram",
		"foo":      "bar",
	}
	for labelKey, labelValue := range externalLabels {
		metrics[model.LabelName(labelKey)] = model.LabelValue(labelValue)
	}

	sh := histogramToSampleHistogram(histogram)

	return model.Vector{
		&model.Sample{
			Metric:    metrics,
			Histogram: &sh,
		},
	}
}

func expectedHistogramModelMatrix(histograms []*histogram.Histogram, startTime time.Time, externalLabels map[string]string) model.Matrix {
	metrics := model.Metric{
		"__name__": "fake_histogram",
		"foo":      "bar",
	}
	for labelKey, labelValue := range externalLabels {
		metrics[model.LabelName(labelKey)] = model.LabelValue(labelValue)
	}

	shp := make([]model.SampleHistogramPair, 0, len(histograms))

	for i, h := range histograms {
		shp = append(shp, model.SampleHistogramPair{
			Timestamp: model.Time(startTime.Add(time.Duration(i) * 30 * time.Second).UnixMilli()),
			Histogram: histogramToSampleHistogram(h),
		})
	}

	return model.Matrix{
		&model.SampleStream{
			Metric:     metrics,
			Histograms: shp,
		},
	}
}

func histogramToSampleHistogram(h *histogram.Histogram) model.SampleHistogram {
	var buckets []*model.HistogramBucket

	buckets = append(buckets, bucketToSampleHistogramBucket(h.ZeroBucket()))

	it := h.PositiveBucketIterator()
	for it.Next() {
		buckets = append(buckets, bucketToSampleHistogramBucket(it.At()))
	}

	return model.SampleHistogram{
		Count:   model.FloatString(h.Count),
		Sum:     model.FloatString(h.Sum),
		Buckets: buckets,
	}
}

func bucketToSampleHistogramBucket(bucket histogram.Bucket[uint64]) *model.HistogramBucket {
	return &model.HistogramBucket{
		Lower:      model.FloatString(bucket.Lower),
		Upper:      model.FloatString(bucket.Upper),
		Count:      model.FloatString(bucket.Count),
		Boundaries: boundaries(bucket),
	}
}

func boundaries(bucket histogram.Bucket[uint64]) int {
	switch {
	case bucket.UpperInclusive && !bucket.LowerInclusive:
		return 0
	case !bucket.UpperInclusive && bucket.LowerInclusive:
		return 1
	case !bucket.UpperInclusive && !bucket.LowerInclusive:
		return 2
	default:
		return 3
	}
}

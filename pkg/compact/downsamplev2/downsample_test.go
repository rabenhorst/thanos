package downsamplev2

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestQueryDownsample(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	logger := log.NewLogfmtLogger(os.Stderr)

	id, err := e2eutil.CreateBlock(
		ctx,
		dir,
		[]labels.Labels{
			{{Name: labels.MetricName, Value: "a"}, {Name: "a", Value: "1"}},
			{{Name: labels.MetricName, Value: "a"}, {Name: "a", Value: "2"}},
		},
		9600, 0, downsample.ResLevel2DownsampleRange+1, // Pass the minimum ResLevel1DownsampleRange check.
		labels.Labels{},
		downsample.ResLevel0, metadata.NoneFunc)
	testutil.Ok(t, err)

	bdir := fmt.Sprintf("%v/%v", dir, id.String())
	pool := chunkenc.NewPool()
	b, err := tsdb.OpenBlock(logger, bdir, pool)
	testutil.Ok(t, err)
	meta, err := metadata.ReadFromDir(bdir)
	testutil.Ok(t, err)

	ctx = context.Background()
	uid, err := Downsample(ctx, logger, meta, b, dir, downsample.ResLevel1)
	testutil.Ok(t, err)
	fmt.Println(uid)

	res1bdir := fmt.Sprintf("%v/%v", dir, uid.String())
	res1b, err := tsdb.OpenBlock(logger, res1bdir, pool)
	testutil.Ok(t, err)
	res1meta, err := metadata.ReadFromDir(res1bdir)
	testutil.Ok(t, err)

	res1uid, err := Downsample(ctx, logger, res1meta, res1b, dir, downsample.ResLevel2)
	testutil.Ok(t, err)

	res1uidBlockDir := filepath.Join(dir, res1uid.String())
	bRes1uid, err := tsdb.OpenBlock(logger, res1uidBlockDir, pool)
	testutil.Ok(t, err)

	indexr, err := bRes1uid.Index()
	testutil.Ok(t, err)
	postings, err := indexr.Postings(index.AllPostingsKey())

	expectedLabels := []labels.Labels{
		{{Name: labels.MetricName, Value: "a"}, {Name: "a", Value: "1"}},
		{{Name: labels.MetricName, Value: "a"}, {Name: "a", Value: "2"}},
	}

	i := 0
	for postings.Next() {
		var (
			chks    []chunks.Meta
			builder labels.ScratchBuilder
		)

		testutil.Ok(t, indexr.Series(postings.At(), &builder, &chks))
		lset := builder.Labels()
		testutil.Equals(t, expectedLabels[i], lset)
		testutil.Equals(t, int64(downsample.ResLevel2DownsampleRange), chks[len(chks)-1].MaxTime)
		// TODO check expected samples.
		i++
	}

}

func TestQueryString(t *testing.T) {
	tt := []struct {
		aggrType downsample.AggrType
		res      time.Duration
		lset     labels.Labels
		qs       string
	}{
		{
			aggrType: downsample.AggrCounter,
			res:      5 * time.Minute,
			lset:     labels.FromStrings(labels.MetricName, "a", "a", "1"),
			qs:       `increase({__name__="a",a="1"}[5m0s])`,
		}, {
			aggrType: downsample.AggrSum,
			res:      5 * time.Minute,
			lset:     labels.FromStrings(labels.MetricName, "a", "a", "1"),
			qs:       `sum({__name__="a",a="1"})`,
		},
	}

	for _, tc := range tt {
		t.Run(tc.qs, func(t *testing.T) {
			qs := queryString(tc.aggrType, tc.res, tc.lset)
			testutil.Equals(t, tc.qs, qs)
		})
	}
}

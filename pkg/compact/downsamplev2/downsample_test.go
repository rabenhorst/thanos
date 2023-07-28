package downsamplev2

import (
	"context"
	"fmt"
	"os"
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

func TestDownsample(t *testing.T) {
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
	fmt.Println(res1uid)

}

//func TestQueryString(t *testing.T) {
//	qs := queryString("increase", "5m", labels.FromStrings(labels.MetricName, "a", "a", "1"))
//	testutil.Equals(t, `increase({__name__="a",a="1"}[5m])`, qs)
//}

//func TestQueryDownsampled(t *testing.T) {
//	bs, err := store.NewBucketStore(
//		log.NewNopLogger(),
//	)
//
//	bs.
//
//}

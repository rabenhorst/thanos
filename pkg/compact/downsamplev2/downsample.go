package downsamplev2

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/runutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	maxSamplesPerChunk = 120
)

var (
	promAggregators = map[downsample.AggrType][2]string{
		downsample.AggrCount:   {"count_over_time", "5m"},
		downsample.AggrSum:     {"sum", ""},
		downsample.AggrMin:     {"min", ""},
		downsample.AggrMax:     {"max", ""},
		downsample.AggrCounter: {"increase", "5m"},
	}
)

type blockQuerier struct {
	q storage.Querier
}

func (b *blockQuerier) Querier(context.Context, int64, int64) (storage.Querier, error) {
	return b.q, nil
}

func Downsample(
	ctx context.Context,
	logger log.Logger,
	origMeta *metadata.Meta,
	b tsdb.BlockReader,
	dir string,
	resolution int64,
) (id ulid.ULID, err error) {
	if origMeta.Thanos.Downsample.Resolution >= resolution {
		return id, errors.New("target resolution not lower than existing one")
	}

	indexr, err := b.Index()
	if err != nil {
		return id, errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithErrCapture(&err, indexr, "downsample index reader")

	// Generate new block id.
	uid := ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))

	// Create block directory to populate with chunks, meta and index files into.
	blockDir := filepath.Join(dir, uid.String())
	if err := os.MkdirAll(blockDir, 0750); err != nil {
		return id, errors.Wrap(err, "mkdir block dir")
	}

	// Remove blockDir in case of errors.
	defer func() {
		if err != nil {
			var merr errutil.MultiError
			merr.Add(err)
			merr.Add(os.RemoveAll(blockDir))
			err = merr.Err()
		}
	}()

	// Copy original meta to the new one. Update downsampling resolution and ULID for a new block.
	newMeta := *origMeta
	newMeta.Thanos.Downsample.Resolution = resolution
	newMeta.ULID = uid

	// Writes downsampled chunks right into the files, avoiding excess memory allocation.
	// Flushes index and meta data after aggregations.
	streamedBlockWriter, err := downsample.NewStreamedBlockWriter(blockDir, indexr, logger, newMeta)
	if err != nil {
		return id, errors.Wrap(err, "get streamed block writer")
	}
	defer runutil.CloseWithErrCapture(&err, streamedBlockWriter, "close stream block writer")

	if origMeta.Thanos.Downsample.Resolution == 0 {
		q, err := tsdb.NewBlockQuerier(b, b.Meta().MinTime, b.Meta().MaxTime)
		if err != nil {
			return id, errors.Wrap(err, "create querier")
		}

		// Get all series from the block.
		seriesSet := q.Select(false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))

		// Create new promql engine.
		opts := promql.EngineOpts{
			Logger:        logger,
			Reg:           nil,
			MaxSamples:    math.MaxInt32,
			Timeout:       30 * time.Second,
			LookbackDelta: 5 * time.Minute,
		}
		ng := engine.New(engine.Opts{EngineOpts: opts})

		for seriesSet.Next() {
			lset := seriesSet.At().Labels()
			chks, err := downsampleRawSeries(ctx, &blockQuerier{q: q}, ng, lset, time.UnixMilli(b.Meta().MinTime), time.UnixMilli(b.Meta().MaxTime))
			if err != nil {
				return id, errors.Wrapf(err, "downsample series: %v", lset.String())
			}
			if err := streamedBlockWriter.WriteSeries(lset, chks); err != nil {
				return id, errors.Wrapf(err, "write series: %v", lset.String())
			}
		}

	} else {
		return id, errors.New("downsampling of already downsampled block is not supported yet")
	}
	return uid, nil
}

func downsampleRawSeries(
	ctx context.Context,
	q storage.Queryable,
	ng v1.QueryEngine,
	lset labels.Labels,
	bMint, bMaxt time.Time,
) ([]chunks.Meta, error) {
	var (
		aggrSeries [5]*promql.Series
		err        error
	)
	for i := 0; i <= int(downsample.AggrCounter); i++ {
		aggrSeries[i], err = querySingleSeries(ctx, ng, q, queryString(promAggregators[downsample.AggrType(i)][0], promAggregators[downsample.AggrType(i)][1], lset), bMint, bMaxt, 5*time.Minute)
		if err != nil {
			return nil, err
		}
		// TODO: query first value of counter
		if i == int(downsample.AggrCounter) {
			aggrSeries[i].Floats = append([]promql.FPoint{{T: 0, F: 0}}, aggrSeries[i].Floats...)
		}
	}

	if !aligned(aggrSeries) {
		return nil, errors.New("series aggregates are not aligned")
	}

	var aggrChunks []chunks.Meta

	for i := 0; i < len(aggrSeries[downsample.AggrCounter].Floats); i += 120 {
		to := i + 119
		if to > len(aggrSeries[downsample.AggrCounter].Floats) {
			to = len(aggrSeries[downsample.AggrCounter].Floats)
		}
		aggrChunks = append(aggrChunks, downsampleFloatBatch(aggrSeries, int64(i), int64(to)))
	}

	return aggrChunks, nil
}

func queryString(aggregator string, vectorRange string, series labels.Labels) string {
	builder := strings.Builder{}
	builder.WriteString(aggregator)
	builder.WriteString("({")
	for i, l := range series {
		builder.WriteString(fmt.Sprintf("%s=\"%s\"", l.Name, l.Value))
		if i != len(series)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString("}")
	if vectorRange != "" {
		builder.WriteString("[")
		builder.WriteString(vectorRange)
		builder.WriteString("]")
	}
	builder.WriteString(")")
	return builder.String()
}

func querySingleSeries(ctx context.Context, ng v1.QueryEngine, q storage.Queryable, qs string, mint, maxt time.Time, step time.Duration) (*promql.Series, error) {
	query, err := ng.NewRangeQuery(ctx, q, &promql.QueryOpts{}, qs, mint, maxt, step)
	if err != nil {
		return nil, err
	}
	sqres := query.Exec(ctx)
	if sqres.Err != nil {
		return nil, sqres.Err
	}
	var res promql.Series
	switch sqres.Value.Type() {
	case parser.ValueTypeMatrix:
		matrix := sqres.Value.(promql.Matrix)
		if len(matrix) > 1 {
			return nil, errors.New("more than one series returned")
		}
		res = matrix[0]
	default:
		return nil, errors.New("unknown result type")
	}
	return &res, nil
}

func downsampleFloatBatch(aggrSeries [5]*promql.Series, from, to int64) chunks.Meta {
	var (
		aggrChunks [5]chunkenc.Chunk
		aggrApps   [5]chunkenc.Appender
	)

	aggrChunks[downsample.AggrCount] = chunkenc.NewXORChunk()
	aggrChunks[downsample.AggrSum] = chunkenc.NewXORChunk()
	aggrChunks[downsample.AggrMin] = chunkenc.NewXORChunk()
	aggrChunks[downsample.AggrMax] = chunkenc.NewXORChunk()
	aggrChunks[downsample.AggrCounter] = chunkenc.NewXORChunk()

	for i, c := range aggrChunks {
		if c != nil {
			aggrApps[i], _ = c.Appender()
		}
	}

	// A panic here means that series aggregates are not aligned.
	for i := from; i < to; i++ {
		for i := 0; i <= int(downsample.AggrCounter); i++ {
			aggrApps[i].Append(aggrSeries[i].Floats[i].T, aggrSeries[i].Floats[i].F)
		}
	}

	return chunks.Meta{
		MinTime: aggrSeries[downsample.AggrCounter].Floats[from].T,
		MaxTime: aggrSeries[downsample.AggrCounter].Floats[to-1].T,
		Chunk:   downsample.EncodeAggrChunk(aggrChunks),
	}
}

func aligned(series [5]*promql.Series) bool {
	for _, s := range series {
		if len(s.Floats) != len(series[0].Floats) {
			return false
		}
		if s.Floats[0].T != series[0].Floats[0].T {
			return false
		}
		if s.Floats[len(s.Floats)-1].T != series[0].Floats[len(series[0].Floats)-1].T {
			return false
		}
	}
	return true
}

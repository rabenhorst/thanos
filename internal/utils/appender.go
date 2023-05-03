// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package utils

import (
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/thanos-io/thanos/internal/mimir-prometheus/tsdb/chunkenc"
)

type RecodingAppender struct {
	chk *chunkenc.Chunk
	app chunkenc.Appender
}

func NewRecodingAppender(chk *chunkenc.Chunk, app chunkenc.Appender) *RecodingAppender {
	return &RecodingAppender{
		chk: chk,
		app: app,
	}
}

func (a *RecodingAppender) Append(t int64, v float64) {
	a.app.Append(t, v)
}

func (a *RecodingAppender) AppendHistogram(t int64, h *histogram.Histogram) bool {
	app, _ := a.app.(*chunkenc.HistogramAppender)

	if app.NumSamples() > 0 {
		var (
			pForwardInserts, nForwardInserts   []chunkenc.Insert
			pBackwardInserts, nBackwardInserts []chunkenc.Insert
			pMergedSpans, nMergedSpans         []histogram.Span
			okToAppend                         bool
		)
		switch h.CounterResetHint {
		case histogram.GaugeType:
			if app != nil {
				pForwardInserts, nForwardInserts,
					pBackwardInserts, nBackwardInserts,
					pMergedSpans, nMergedSpans,
					okToAppend = app.AppendableGauge(h)
			}
		default:
			if app != nil {
				pForwardInserts, nForwardInserts, okToAppend, _ = app.Appendable(h)
			}
		}
		if !okToAppend {
			return false
		}

		if len(pBackwardInserts)+len(nBackwardInserts) > 0 {
			h.PositiveSpans = pMergedSpans
			h.NegativeSpans = nMergedSpans
			app.RecodeHistogram(h, pBackwardInserts, nBackwardInserts)
		}
		if len(pForwardInserts) > 0 || len(nForwardInserts) > 0 {
			chk, app := app.Recode(
				pForwardInserts, nForwardInserts,
				h.PositiveSpans, h.NegativeSpans,
			)
			*a.chk = chk
			a.app = app
		}
	}

	a.app.AppendHistogram(t, h)
	return true
}

func (a *RecodingAppender) AppendFloatHistogram(t int64, fh *histogram.FloatHistogram) bool {
	app, _ := a.app.(*chunkenc.FloatHistogramAppender)

	if app.NumSamples() > 0 {
		var (
			pForwardInserts, nForwardInserts   []chunkenc.Insert
			pBackwardInserts, nBackwardInserts []chunkenc.Insert
			pMergedSpans, nMergedSpans         []histogram.Span
			okToAppend                         bool
		)
		switch fh.CounterResetHint {
		case histogram.GaugeType:
			pForwardInserts, nForwardInserts,
				pBackwardInserts, nBackwardInserts,
				pMergedSpans, nMergedSpans,
				okToAppend = app.AppendableGauge(fh)
		default:
			if app != nil {
				pForwardInserts, nForwardInserts, okToAppend, _ = app.Appendable(fh)
			}
		}

		if !okToAppend {
			return false
		}

		if len(pBackwardInserts)+len(nBackwardInserts) > 0 {
			fh.PositiveSpans = pMergedSpans
			fh.NegativeSpans = nMergedSpans
			app.RecodeHistogramm(fh, pBackwardInserts, nBackwardInserts)
		}

		if len(pForwardInserts) > 0 || len(nForwardInserts) > 0 {
			chunk, app := app.Recode(
				pForwardInserts, nForwardInserts,
				fh.PositiveSpans, fh.NegativeSpans,
			)
			*a.chk = chunk
			a.app = app
		}
	}

	a.app.AppendFloatHistogram(t, fh)
	return true
}

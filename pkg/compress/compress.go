package compress

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

const ThresholdBytes = 256

type Encoder struct {
	pool sync.Pool
}

type Decoder struct {
	pool sync.Pool
}

func NewEncoder() (*Encoder, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	enc.Close()

	e := &Encoder{}
	e.pool = sync.Pool{
		New: func() any {
			w, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
			return w
		},
	}
	return e, nil
}

func NewDecoder() (*Decoder, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	dec.Close()

	d := &Decoder{}
	d.pool = sync.Pool{
		New: func() any {
			r, _ := zstd.NewReader(nil)
			return r
		},
	}
	return d, nil
}

func (e *Encoder) Compress(src []byte) ([]byte, bool, error) {
	if len(src) <= ThresholdBytes {
		return src, false, nil
	}

	enc := e.pool.Get().(*zstd.Encoder)
	dst := enc.EncodeAll(src, make([]byte, 0, len(src)/2))
	e.pool.Put(enc)

	return dst, true, nil
}

func (d *Decoder) Decompress(src []byte) ([]byte, error) {
	dec := d.pool.Get().(*zstd.Decoder)
	dst, err := dec.DecodeAll(src, nil)
	d.pool.Put(dec)
	return dst, err
}

func (e *Encoder) Close() {
}

func (d *Decoder) Close() {
}

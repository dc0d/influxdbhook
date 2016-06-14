//Package influxdbhook provides a logrus hook for sending logs to InfluxDB
package influxdbhook

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/influxdata/influxdb/client/v2"
)

//InitInfluxDBHook creates & initializes a InfluxDBHook
func InitInfluxDBHook(
	c client.Client,
	batchPointsConfig client.BatchPointsConfig,
	metric string,
	tags []string,
	batchInterval time.Duration,
	batchSize int,
	levels []logrus.Level) *InfluxDBHook {
	r := new(InfluxDBHook)

	r._client = c
	r._batchPointsConfig = batchPointsConfig
	r._metric = metric
	r._tags = tags
	r._batchInterval = batchInterval
	r._batchSize = batchSize
	_l := levels
	if len(_l) == 0 {
		_l = defaultLevels()
	}
	r._levels = _l

	r._points = make(chan *client.Point, r._batchSize)
	go r.loop()

	return r
}

//InfluxDBHook is a logrus hook sends logs to InfluxDB
type InfluxDBHook struct {
	_client            client.Client
	_batchPointsConfig client.BatchPointsConfig
	_metric            string
	_tags              []string
	_batchInterval     time.Duration
	_batchSize         int
	_levels            []logrus.Level

	_points chan *client.Point
}

func (h *InfluxDBHook) loop() {
	var coll []*client.Point
	tick := time.NewTicker(h._batchInterval)

	for {
		timeout := false

		select {
		case pt := <-h._points:
			coll = append(coll, pt)
		case <-tick.C:
			timeout = true
		}

		if (timeout || len(coll) >= h._batchSize) && len(coll) > 0 {
			bp, err := client.NewBatchPoints(h._batchPointsConfig)
			if err != nil {
				//TODO:
			}
			bp.AddPoints(coll)
			err = h._client.Write(bp)
			if err != nil {
				//TODO:
			} else {
				coll = nil
			}
		}
	}
}

//Levels implementation of interface logrus.Hook
func (h *InfluxDBHook) Levels() []logrus.Level {
	return h._levels
}

//Fire implementation of interface logrus.Hook
func (h *InfluxDBHook) Fire(entry *logrus.Entry) error {
	fields := make(map[string]interface{})
	tags := make(map[string]string)

	fields[`message`] = entry.Message
	tags[`level`] = entry.Level.String()

	for _, tag := range h._tags {
		if tagValue, ok := getVal(entry.Data, tag); ok {
			tags[tag] = tagValue
		}
	}

	for k, v := range entry.Data {
		fields[k] = v
	}
	for _, tag := range h._tags {
		delete(fields, tag)
	}

	pt, err := client.NewPoint(h._metric, tags, fields, entry.Time)
	if err != nil {
		return err
	}

	h._points <- pt

	return nil
}

func getVal(fields logrus.Fields, key string) (string, bool) {
	value, ok := fields[key]
	if ok {
		return fmt.Sprintf("%v", value), ok
	}
	return "", ok
}

func defaultLevels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

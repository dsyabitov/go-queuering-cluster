package main

import (
	"encoding/json"
	"math/rand/v2"
)

type Data struct {
	ParamID int64   `json:"param_id"`
	Value   float64 `json:"value"`
}

func (d *Data) Serialize() []byte {
	data, err := json.Marshal(d)
	if err != nil {
		panic(err) // should no error here
	}
	return data
}

type DataGen struct {
	MinID    int64
	MaxID    int64
	MinValue float64
	MaxValue float64
}

func (g *DataGen) Next() Data {
	return Data{
		int64(rand.IntN(int(g.MaxID-g.MinID))) + g.MinID,
		rand.Float64()*10 + g.MinValue,
	}
}

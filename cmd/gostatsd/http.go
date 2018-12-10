package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type httpStatus struct {
	Service string `json:"service"`
	Name    string `json:"name"`
	Status  string `json:"status"`
}

func (status *httpStatus) handle(responseWriter http.ResponseWriter, _r *http.Request) {
	// assuming some checks may happen here
	status.Status = "OK"
	responseWriter.WriteHeader(http.StatusOK)
	responseWriter.Header().Set("Content-Type", "application/json")
	statusData, err := json.Marshal(status)
	if err == nil {
		io.WriteString(responseWriter, string(statusData)+"\n")
	} else {
		logrus.Errorf("Unable to marchall JSON status: %v", err)
	}
}

func runHTTPServer(v *viper.Viper) {
	var err error
	address := v.GetString(ParamProfile)
	name := v.GetString(ParamProfileName)

	if name == "" {
		name, err = os.Hostname()
		if err != nil {
			logrus.Errorf("Could not get the system hostname: %v", err)
		}
	}
	if address == "" {
		address = "127.0.0.1:8126"
	}

	status := httpStatus{
		Service: EnvPrefix,
		Name:    name,
	}

	http.HandleFunc("/status", status.handle)

	logrus.Info("Starting HTTP server on: ", address)
	err = http.ListenAndServe(address, nil)
	if err != nil {
		logrus.Errorf("HTTP server failed: %v", err)
	}
}

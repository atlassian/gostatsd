package statsdaemon

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

func getTLSConfiguration(caPath, certPath, keyPath string, enable bool) (*tls.Config, error) {
	if !enable {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Rationale:
		// * Retain performance-conscious ordering from the crypto/tls defaults
		// * Reject deprecated ciphers and modes, including non-DHE
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		},
	}

	if caPath != "" {
		caPEM, err := ioutil.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("[%s] error reading tls ca: %v", BackendName, err)
		}

		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM(caPEM); !ok {
			return nil, fmt.Errorf("[%s] error reading tls ca: no certs found", BackendName)
		}
	}

	if certPath != "" || keyPath != "" {
		if certPath == "" {
			return nil, fmt.Errorf("[%s] tls_cert_path is required when tls_key_path is set", BackendName)
		}
		if keyPath == "" {
			return nil, fmt.Errorf("[%s] tls_key_path is required when tls_cert_path is set", BackendName)
		}

		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("[%s] error loading client certificate: %v", BackendName, err)
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
	}

	return tlsConfig, nil
}

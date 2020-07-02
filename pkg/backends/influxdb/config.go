package influxdb

import (
	"errors"
	"net/url"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	paramApiVersion      = "api-version"
	paramBucket          = "bucket"
	paramConsistency     = "consistency"
	paramDatabase        = "database"
	paramOrg             = "org"
	paramRetentionPolicy = "retention-policy"

	queryBucket          = "bucket"
	queryConsistency     = "consistency"
	queryDatabase        = "database"
	queryOrg             = "org"
	queryRetentionPolicy = "rp"
)

var (
	errUnknownVersion   = errors.New("[" + BackendName + "] " + paramApiVersion + " must be 1 or 2")
	errBucketRequired   = errors.New("[" + BackendName + "] " + paramBucket + " is required")
	errDatabaseRequired = errors.New("[" + BackendName + "] " + paramDatabase + " is required")
	errOrgRequired      = errors.New("[" + BackendName + "] " + paramOrg + " is required")
)

type config interface {
	Path() string
	Build(url.Values)
}

type configV1 struct {
	database        string
	retentionPolicy string
	consistency     string
}

type configV2 struct {
	bucket string
	org    string
}

func (v1 configV1) Path() string {
	return "/write"
}

func (v1 configV1) Build(q url.Values) {
	q.Set(queryDatabase, v1.database)
	if v1.consistency != "" {
		q.Set(queryConsistency, v1.consistency)
	}
	if v1.retentionPolicy != "" {
		q.Set(queryRetentionPolicy, v1.retentionPolicy)
	}
}

func (v2 configV2) Path() string {
	return "/api/v2/write"
}

func (v2 configV2) Build(q url.Values) {
	q.Set(queryBucket, v2.bucket)
	q.Set(queryOrg, v2.org)
}

func newConfigFromViper(v *viper.Viper, logger logrus.FieldLogger) (config, error) {
	v.SetDefault(paramApiVersion, 2)
	v.SetDefault(paramBucket, "")          // v2
	v.SetDefault(paramConsistency, "")     // v1
	v.SetDefault(paramDatabase, "")        // v1
	v.SetDefault(paramOrg, "")             // v2
	v.SetDefault(paramRetentionPolicy, "") // v1

	apiVersion := v.GetInt(paramApiVersion)
	bucket := v.GetString(paramBucket)                   // v2
	consistency := v.GetString(paramConsistency)         // v1
	database := v.GetString(paramDatabase)               // v1
	org := v.GetString(paramOrg)                         // v2
	retentionPolicy := v.GetString(paramRetentionPolicy) // v1

	switch apiVersion {
	case 1:
		return newConfigV1(bucket, consistency, database, org, retentionPolicy, logger)
	case 2:
		return newConfigV2(bucket, consistency, database, org, retentionPolicy, logger)
	default:
		return nil, errUnknownVersion
	}
}

func newConfigV1(bucket, consistency, database, org, retentionPolicy string, logger logrus.FieldLogger) (config, error) {
	if bucket != "" {
		logger.WithField(paramBucket, bucket).Warn(paramBucket + " is not applicable in " + paramApiVersion + " 1")
	}
	if consistency != "any" && consistency != "one" && consistency != "quorum" && consistency != "all" && consistency != "" {
		logger.WithField(paramConsistency, consistency).Warn(paramConsistency + " is not a recognized value (any, one, quorum, all, or not specified")
	}
	if org != "" {
		logger.WithField(paramOrg, org).Warn(paramOrg + " is not applicable in " + paramApiVersion + " 1")
	}

	if database == "" {
		return nil, errDatabaseRequired
	}

	// retentionPolicy may be blank, no checks

	logger.WithFields(logrus.Fields{
		paramApiVersion:      1,
		paramConsistency:     consistency,
		paramDatabase:        database,
		paramRetentionPolicy: retentionPolicy,
	}).Info("created configuration")

	return configV1{
		consistency:     consistency,
		database:        database,
		retentionPolicy: retentionPolicy,
	}, nil
}

func newConfigV2(bucket, consistency, database, org, retentionPolicy string, logger logrus.FieldLogger) (config, error) {
	if consistency != "" {
		// Is this accurate? I can't see documentation for it.  The official v1 client
		// has it, but the v2 client does not.  2.0 is still beta, so presumably there
		// is no enterprise version of it, so this will probably change in the future.
		logger.WithField(paramConsistency, consistency).Warn(paramConsistency + " is not applicable in " + paramApiVersion + " 2")
	}
	if database != "" {
		logger.WithField(paramDatabase, database).Warn(paramDatabase + " is not applicable in " + paramApiVersion + " 2")
	}
	if retentionPolicy != "" {
		logger.WithField(paramRetentionPolicy, retentionPolicy).Warn(paramRetentionPolicy + " is not applicable in " + paramApiVersion + " 2")
	}

	if bucket == "" {
		return nil, errBucketRequired
	}
	if org == "" {
		return nil, errOrgRequired
	}

	logger.WithFields(logrus.Fields{
		paramApiVersion: 2,
		paramBucket:     bucket,
		paramOrg:        org,
	}).Info("created configuration")

	return configV2{
		bucket: bucket,
		org:    org,
	}, nil
}

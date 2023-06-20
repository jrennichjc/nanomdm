package main

import (
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"

	"github.com/micromdm/nanomdm/certverify"
	"github.com/micromdm/nanomdm/cli"
	mdmhttp "github.com/micromdm/nanomdm/http"
	httpapi "github.com/micromdm/nanomdm/http/api"
	httpmdm "github.com/micromdm/nanomdm/http/mdm"
	"github.com/micromdm/nanomdm/log/stdlogfmt"
	"github.com/micromdm/nanomdm/push/buford"
	pushsvc "github.com/micromdm/nanomdm/push/service"
	"github.com/micromdm/nanomdm/service"
	"github.com/micromdm/nanomdm/service/certauth"
	"github.com/micromdm/nanomdm/service/dump"
	"github.com/micromdm/nanomdm/service/microwebhook"
	"github.com/micromdm/nanomdm/service/multi"
	"github.com/micromdm/nanomdm/service/nanomdm"
)

// overridden by -ldflags -X
var version = "unknown"

const (
	endpointMDM     = "/mdm"
	endpointCheckin = "/checkin"

	endpointAPIPushCert  = "/v1/pushcert"
	endpointAPIPush      = "/v1/push/"
	endpointAPIEnqueue   = "/v1/enqueue/"
	endpointAPIMigration = "/migration"
	endpointAPIVersion   = "/version"
)

func main() {
	cliStorage := cli.NewStorage()
	flag.Var(&cliStorage.Storage, "storage", "name of storage backend")
	flag.Var(&cliStorage.DSN, "storage-dsn", "data source name (e.g. connection string or path)")
	flag.Var(&cliStorage.DSN, "dsn", "data source name; deprecated: use -storage-dsn")
	flag.Var(&cliStorage.Options, "storage-options", "storage backend options")
	var (
		flAPIKey     = flag.String("api", "", "API key for API endpoints")
		flVersion    = flag.Bool("version", false, "print version")
		flWebhook    = flag.String("webhook-url", "", "URL to send requests to")
		flCertHeader = flag.String("cert-header", "", "HTTP header containing URL-escaped TLS client certificate")
		flDebug      = flag.Bool("debug", false, "log debug messages")
		flDump       = flag.Bool("dump", false, "dump MDM requests and responses to stdout")
		flDisableMDM = flag.Bool("disable-mdm", false, "disable MDM HTTP endpoint")
		flCheckin    = flag.Bool("checkin", false, "enable separate HTTP endpoint for MDM check-ins")
		flMigration  = flag.Bool("migration", false, "HTTP endpoint for enrollment migrations")
		flRetro      = flag.Bool("retro", false, "Allow retroactive certificate-authorization association")
		flDMURLPfx   = flag.String("dm", "", "URL to send Declarative Management requests to")
	)
	flag.Parse()

	if *flVersion {
		fmt.Println(version)
		return
	}

	logger := stdlogfmt.New(stdlogfmt.WithDebugFlag(*flDebug))

	caPemString := `-----BEGIN CERTIFICATE-----
MIIFVTCCAz2gAwIBAgIBATANBgkqhkiG9w0BAQsFADBMMQswCQYDVQQGEwJVUzEQ
MA4GA1UEChMHc2NlcC1jYTEQMA4GA1UECxMHU0NFUCBDQTEZMBcGA1UEAxMQTUlD
Uk9NRE0gU0NFUCBDQTAeFw0yMjEwMTIyMjI0NTVaFw0zMjEwMTIyMjI0NTVaMEwx
CzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdzY2VwLWNhMRAwDgYDVQQLEwdTQ0VQIENB
MRkwFwYDVQQDExBNSUNST01ETSBTQ0VQIENBMIICIjANBgkqhkiG9w0BAQEFAAOC
Ag8AMIICCgKCAgEAyYaDaPgfPzyuEuxvd4Knjrx5uR7/m7J339JWulOQAVK9MXu1
rRKQElJcG4RUhsWEtdU3hY7TXowpL1Mr1YEBCP91NaodDgv8NyuwWLN8SvxtbiFh
13WLIikfDeVz45ym8aqK8aCblydhxFL7JKSm+1o9kvlXZkllxcqlK+WXtcoIvM2A
CeaQrweL/CYjg+aQ0AHnlMQ5N1dtqfuVa7xuxsIl6aF7WEM6506d+kPcUdJlEvqA
dKSMo3Kk3CIETpTFPZbLVJwjZ28K9kPpeXqpfGx/gqEn9ISgnAt8QDLDhJcCU59H
+yY8b5047eepg6VViuPMhbnRI/kZrrppmoPioXODmHYsX9s7wnzaWKknojhyF9bw
U0Khw2YP6agxp5450L/ZEHOFbpXnse63c2E9S27MZSRDJVpRX4Yvm5SNi+2MFMMN
HF1AdV2PEo2EM4tL1GE4zmwciQvQ/A2IsvaWFx9gFIJ2qnzm6U+al7P/sm2s/moj
fwfi7pzOoL1V9E3w8rUHptb5tK/V2CugoMVn2jxV4RmotWpqg4MKqzeH05USAFKK
X8Jc1kNx3owIsvCYvDPT/Ruhq4cf2es5LcDOajS/uPN8L8Dqc6nRVMdoOyiaTHey
zbdm+X7OsRIduNG0nyfJ+nVfQsejz/v7VC87Qf2nKjH9vq4DpE9XtlLnyUMCAwEA
AaNCMEAwDgYDVR0PAQH/BAQDAgEGMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYE
FHssG/AIcUub51TO/z+BynJ9bc/BMA0GCSqGSIb3DQEBCwUAA4ICAQB8QqBTABAb
cJ2d2Qmr9clWLG+dFmVxVRxVLCLi+3Pi9YdZP5UDpVZ2NGzJTJNWA1sM/ea+26m5
Gk5Um/gKsd7xlIep+GItG6VmrTz758feLNS0tYnhfnRl+scgdkEPStHzaBlTc8iL
95LIIkeUXZt4cH3cgmiL2vXL3v6+CCTiS+aH2zI8g27Bxige52MMYgr0t8ci1Gf4
mP3pkKXyXJbm15l3sWNT8jVC+rmuPY8RjkU/APXCZsT7xzRFVJ3iFgyqms7PzuMc
p/tG9ITvCL/DgE1AlJdZhQ7U6vvCVX20yOsT5bc3DL4yixtKF+ZUWEOv3zbnFc91
+yI7ZOUPf9JLpfQLYZ/k3/lEgRHqFgmyAiHRvNtEnpqEQ4f+GXOapbvjJUWPS2Qn
1sVSwnaKms6j6ZmSPbpPzEpubvDE0f+19y6c/jGlB+Vmof9Tm5GRd6z6/SjuyyGG
/QmgtckOEQlomBHQOhT0DYfcOtIM+HQfxdFNyTBEZPvB9bf5RzQyQ9arTlLd2TM8
WirYzALEHQmqTKwoymj/UTinjeS3ofc54pf+xh8gqHeVO5P8clis3Ke+WcLeLkIc
HnVcN1b9qZ/ecQ3FY69LGqXGrT9xniozCe1+j/mkFMJQdkuLvvtnVRmdr/Gg2OYY
Tw6AQ7d76RC1Cesgr1wUJMDbvXab89qySA==
-----END CERTIFICATE-----`

	r := strings.NewReader(caPemString)
	caPEM, err := ioutil.ReadAll(r)

	if err != nil {
		stdlog.Fatal(err)
	}

	var intsPEM []byte

	verifier, err := certverify.NewPoolVerifier(caPEM, intsPEM, x509.ExtKeyUsageClientAuth)
	if err != nil {
		stdlog.Fatal(err)
	}

	cliStorage.DSN.Set("nanomdm")
	cliStorage.Storage.Set("dynamo")
	mdmStorage, err := cliStorage.Parse(logger)
	if err != nil {
		stdlog.Fatal(err)
	}

	// create 'core' MDM service
	nanoOpts := []nanomdm.Option{nanomdm.WithLogger(logger.With("service", "nanomdm"))}
	if *flDMURLPfx != "" {
		logger.Debug("msg", "declarative management setup", "url", *flDMURLPfx)
		dm, err := nanomdm.NewDeclarativeManagementHTTPCaller(*flDMURLPfx, http.DefaultClient)
		if err != nil {
			stdlog.Fatal(err)
		}
		nanoOpts = append(nanoOpts, nanomdm.WithDeclarativeManagement(dm))
	}

	
	nano := nanomdm.New(mdmStorage, nanoOpts...)

	mux := http.NewServeMux()

	mux.HandleFunc("/health", CheckHealth())

	if !*flDisableMDM {
		var mdmService service.CheckinAndCommandService = nano
		if *flWebhook != "" {
			webhookService := microwebhook.New(*flWebhook, mdmStorage)
			mdmService = multi.New(logger.With("service", "multi"), mdmService, webhookService)
		}
		certAuthOpts := []certauth.Option{certauth.WithLogger(logger.With("service", "certauth"))}
		if *flRetro {
			certAuthOpts = append(certAuthOpts, certauth.WithAllowRetroactive())
		}
		mdmService = certauth.New(mdmService, mdmStorage, certAuthOpts...)
		if *flDump {
			mdmService = dump.New(mdmService, os.Stdout)
		}

		// register 'core' MDM HTTP handler
		var mdmHandler http.Handler
		if *flCheckin {
			// if we use the check-in handler then only handle commands
			mdmHandler = httpmdm.CommandAndReportResultsHandler(mdmService, logger.With("handler", "command"))
		} else {
			// if we don't use a check-in handler then do both
			mdmHandler = httpmdm.CheckinAndCommandHandler(mdmService, logger.With("handler", "checkin-command"))
		}
		mdmHandler = httpmdm.CertVerifyMiddleware(mdmHandler, verifier, logger.With("handler", "cert-verify"))
		if *flCertHeader != "" {
			mdmHandler = httpmdm.CertExtractPEMHeaderMiddleware(mdmHandler, *flCertHeader, logger.With("handler", "cert-extract"))
		} else {
			mdmHandler = httpmdm.CertExtractMdmSignatureMiddleware(mdmHandler, logger.With("handler", "cert-extract"))
		}
		mux.Handle(endpointMDM, mdmHandler)

		if *flCheckin {
			// if we specified a separate check-in handler, set it up
			var checkinHandler http.Handler
			checkinHandler = httpmdm.CheckinHandler(mdmService, logger.With("handler", "checkin"))
			checkinHandler = httpmdm.CertVerifyMiddleware(checkinHandler, verifier, logger.With("handler", "cert-verify"))
			if *flCertHeader != "" {
				checkinHandler = httpmdm.CertExtractPEMHeaderMiddleware(checkinHandler, *flCertHeader, logger.With("handler", "cert-extract"))
			} else {
				checkinHandler = httpmdm.CertExtractMdmSignatureMiddleware(checkinHandler, logger.With("handler", "cert-extract"))
			}
			mux.Handle(endpointCheckin, checkinHandler)
		}
	}

	if *flAPIKey != "" {
		const apiUsername = "nanomdm"

		// create our push provider and push service
		pushProviderFactory := buford.NewPushProviderFactory()
		pushService := pushsvc.New(mdmStorage, mdmStorage, pushProviderFactory, logger.With("service", "push"))

		// register API handler for push cert storage/upload.
		var pushCertHandler http.Handler
		pushCertHandler = httpapi.StorePushCertHandler(mdmStorage, logger.With("handler", "store-cert"))
		pushCertHandler = mdmhttp.BasicAuthMiddleware(pushCertHandler, apiUsername, *flAPIKey, "nanomdm")
		mux.Handle(endpointAPIPushCert, pushCertHandler)

		// register API handler for push notifications.
		// we strip the prefix to use the path as an id.
		var pushHandler http.Handler
		pushHandler = httpapi.PushHandler(pushService, logger.With("handler", "push"))
		pushHandler = http.StripPrefix(endpointAPIPush, pushHandler)
		pushHandler = mdmhttp.BasicAuthMiddleware(pushHandler, apiUsername, *flAPIKey, "nanomdm")
		mux.Handle(endpointAPIPush, pushHandler)

		// register API handler for new command queueing.
		// we strip the prefix to use the path as an id.
		var enqueueHandler http.Handler
		enqueueHandler = httpapi.RawCommandEnqueueHandler(mdmStorage, pushService, logger.With("handler", "enqueue"))
		enqueueHandler = http.StripPrefix(endpointAPIEnqueue, enqueueHandler)
		enqueueHandler = mdmhttp.BasicAuthMiddleware(enqueueHandler, apiUsername, *flAPIKey, "nanomdm")
		mux.Handle(endpointAPIEnqueue, enqueueHandler)

		if *flMigration {
			// setup a "migration" handler that takes Check-In messages
			// without bothering with certificate auth or other
			// middleware.
			//
			// if the source MDM can put together enough of an
			// authenticate and tokenupdate message to effectively
			// generate "enrollments" then this effively allows us to
			// migrate MDM enrollments between servers.
			var migHandler http.Handler
			migHandler = httpmdm.CheckinHandler(nano, logger.With("handler", "migration"))
			migHandler = mdmhttp.BasicAuthMiddleware(migHandler, apiUsername, *flAPIKey, "nanomdm")
			mux.Handle(endpointAPIMigration, migHandler)
		}
	}

	mux.HandleFunc(endpointAPIVersion, mdmhttp.VersionHandler(version))

	rand.Seed(time.Now().UnixNano())

	logger.Info("msg", "starting server")

	lambda.Start(httpadapter.New(mux).ProxyWithContext)

	logs := []interface{}{"msg", "server shutdown"}
	if err != nil {
		logs = append(logs, "err", err)
	}
	logger.Info(logs...)
}

func CheckHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("I'm alive!"))
	}
}

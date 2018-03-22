package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/coreos/go-systemd/daemon"
	"github.com/temoto/balancer-control"
	"github.com/temoto/senderbender/junk"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var (
		flagAwsQueueUrl = flag.String("aws-queue-url", "", "https://sqs.eu-west-1.amazonaws.com/1234567890/queuename")
	)
	flag.Parse()

	// systemd service
	sdnotify("READY=0\nSTATUS=init\n")
	if wdTime, err := daemon.SdWatchdogEnabled(true); err != nil {
		log.Fatal(err)
	} else if wdTime != 0 {
		go func() {
			for range time.Tick(wdTime) {
				sdnotify("WATCHDOG=1\n")
			}
		}()
	}

	ctx := context.Background()
	ctx = junk.ContextSetMap(ctx, map[string]interface{}{
		// "log-debug": flagDebug,
		"aws-queue-url": flagAwsQueueUrl,
	})
	awsConfig := aws.NewConfig().WithLogger(aws.LoggerFunc(func(args ...interface{}) {
		fmt.Fprintln(os.Stderr, args...)
	})).WithLogLevel(aws.LogDebugWithRequestErrors | aws.LogDebugWithRequestRetries)
	bc := balancercontrol.New()
	bc.ConfigureAws(ctx, awsConfig)

	sigShutdownChan := make(chan os.Signal, 1)
	signal.Notify(sigShutdownChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func(ch <-chan os.Signal, f func()) {
		<-ch
		log.Printf("graceful stop")
		sdnotify("READY=0\nSTATUS=stopping\n")
		f()
	}(sigShutdownChan, bc.Stop)

	sdnotify("READY=1\nSTATUS=work\n")
	bc.Run(ctx)
	bc.Wait()
}

func sdnotify(s string) {
	if _, err := daemon.SdNotify(false, s); err != nil {
		log.Fatal(err)
	}
}

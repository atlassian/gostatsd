FROM alpine:3.24.4
RUN apk --no-cache upgrade && \
    apk --no-cache add \
    ca-certificates
ADD statsd-tester-linux /bin/statsd-tester
ENTRYPOINT ["statsd-tester"]

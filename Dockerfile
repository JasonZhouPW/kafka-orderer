FROM scratch
MAINTAINER Kostas Christidis <kostas@christidis.io>
ADD cmd/server/server server
ADD orderer.yaml orderer.yaml
EXPOSE 5151
CMD ["/server", "-loglevel", "debug", "-verbose", "true"]

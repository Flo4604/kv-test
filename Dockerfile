FROM golang:1.25-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/bench       ./cmd/bench && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/disktier    ./cmd/disktier && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/inv-bakeoff ./cmd/inv-bakeoff && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/inv-driver  ./cmd/inv-driver && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/inv-logical ./cmd/inv-logical && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/inv-notify  ./cmd/inv-notify && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/run-all     ./cmd/run-all

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bench       /usr/local/bin/bench
COPY --from=build /out/disktier    /usr/local/bin/disktier
COPY --from=build /out/inv-bakeoff /usr/local/bin/inv-bakeoff
COPY --from=build /out/inv-driver  /usr/local/bin/inv-driver
COPY --from=build /out/inv-logical /usr/local/bin/inv-logical
COPY --from=build /out/inv-notify  /usr/local/bin/inv-notify
COPY --from=build /out/run-all     /usr/local/bin/run-all

ENTRYPOINT []
# Default: skip disktier. Container ephemeral storage is not
# representative of what a real KV node's disk tier would run on
# (bare-metal NVMe), so the numbers would be misleading. Run
# disktier locally on real disk, or override CMD to include it.
CMD ["/usr/local/bin/run-all", "-skip=disktier"]

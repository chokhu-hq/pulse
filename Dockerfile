FROM --platform=$BUILDPLATFORM golang:1 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/pulse .

FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /out/pulse /usr/local/bin/pulse

ENV PULSE_CONFIG=/etc/pulse/config.json
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/pulse"]

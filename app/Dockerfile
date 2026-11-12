FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go index.html ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /app . && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /app /app
COPY --from=build --chown=nonroot:nonroot /data /data
VOLUME /data
ENV DATA_DIR=/data
EXPOSE 8080
ENTRYPOINT ["/app"]

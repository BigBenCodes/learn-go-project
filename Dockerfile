FROM golang:1.26.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/fraud-service ./cmd/fraud-service

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/fraud-service /app/fraud-service
COPY configs/model.json /app/configs/model.json
EXPOSE 8080
ENTRYPOINT ["/app/fraud-service"]

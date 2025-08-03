# This Dockerfile is only used for local development.
# The actual images are built using ko in GitHub Actions.

# Build stage
FROM golang:1.24-alpine AS builder

# Build arguments
ARG BUILD_PATH

# Build configuration
ENV CGO_ENABLED=0 GOOS=linux

# Install build dependencies
RUN apk update && apk add --no-cache ca-certificates tzdata && update-ca-certificates

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -mod=readonly -o app ${BUILD_PATH}

# Final stage
FROM alpine:latest

# Install ca-certificates, tzdata, and curl (for healthchecks)
RUN apk --no-cache add ca-certificates tzdata curl

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Copy the binary from builder stage with proper ownership
COPY --from=builder --chown=appuser:appgroup /app/app /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

# Run the application
ENTRYPOINT ["/app"]

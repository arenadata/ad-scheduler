# ad-scheduler image. The binary is cross-compiled on the host
# (CGO_ENABLED=0 GOOS=linux) into bin/ad-scheduler, then copied into a minimal
# distroless base — keeps the image tiny and avoids compiling k8s in-container.
FROM gcr.io/distroless/static-debian12:nonroot
COPY bin/ad-scheduler /ad-scheduler
USER 65532:65532
ENTRYPOINT ["/ad-scheduler"]

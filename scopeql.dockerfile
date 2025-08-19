FROM public.ecr.aws/docker/library/rust:1.85.0-bullseye AS build
WORKDIR /build/
ADD . .
RUN ./scopeql/scripts/docker-build.sh

FROM public.ecr.aws/docker/library/debian:bullseye-slim
WORKDIR /app/

COPY --from=build /build/scopeql/target/dist/scopeql /bin/
COPY LICENSE scopeql/README.md /app/

ENTRYPOINT ["/bin/scopeql"]

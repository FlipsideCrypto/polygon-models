FROM ghcr.io/dbt-labs/dbt-snowflake:1.2.latest
WORKDIR /support
RUN mkdir /root/.dbt
COPY profiles.yml /root/.dbt
RUN mkdir /root/flow
WORKDIR /flow
COPY . .
EXPOSE 8080
ENTRYPOINT [ "bash"]

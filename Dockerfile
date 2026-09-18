FROM python:3.12.4-slim-bullseye

COPY . ./roz/

# RUN apt update &&\
#     apt install -y wget \
#     git \
#     openjdk-17-jre-headless

# RUN wget --directory-prefix /opt/bin/ https://github.com/nextflow-io/nextflow/releases/download/v24.04.2/nextflow

# RUN addgroup --gid 1000 jovyan && \
#     adduser --system --shell /bin/false --ingroup jovyan --disabled-password --uid 1000 jovyan

# RUN mkdir /.nextflow \
#     && chmod -R 777 /.nextflow \
#     && chmod 777 /opt/bin/nextflow \
#     && /opt/bin/nextflow info

# RUN chown -R jovyan:jovyan /.nextflow

# Dependencies and their version floors come from roz's setup.cfg, so there is
# nothing to keep in step by hand here
RUN pip3 install ./roz

RUN rm -rf /roz

USER 1000:100

CMD ["/bin/bash"]

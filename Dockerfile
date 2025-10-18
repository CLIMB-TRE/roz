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

RUN pip3 install varys-client

RUN pip3 install climb-onyx-client

RUN pip3 install ./roz

RUN rm -rf /roz

USER jovyan:users

CMD ["/bin/bash"]

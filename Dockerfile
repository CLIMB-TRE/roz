FROM python:3.12-alpine

RUN apk add --no-cache wget \
    git \
    default-jre

RUN wget --directory-prefix /opt/bin/ https://github.com/nextflow-io/nextflow/releases/download/v23.10.0/nextflow 

RUN addgroup --gid 1000 jovyan && \
    adduser --system --shell /bin/false --ingroup jovyan --disabled-password --uid 1000 jovyan

RUN mkdir /.nextflow \
    && chmod -R 777 /.nextflow \
    && chmod 777 /opt/bin/nextflow \
    && /opt/bin/nextflow info

RUN chown -R jovyan:jovyan /.nextflow

ADD "https://api.github.com/repos/CLIMB-TRE/varys/commits?per_page=1" latest_varys_commit
RUN pip3 install git+https://github.com/CLIMB-TRE/varys.git

RUN pip3 install climb-onyx-client

RUN pip3 install .

ENV PATH=/opt/bin:$PATH

USER jovyan

CMD ["/bin/bash"]
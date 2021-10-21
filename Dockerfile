# hadolint ignore=DL3007
FROM python:slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    git \
    wget \
    g++ \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh \
    && echo "Running $(conda --version)" && \
    conda init bash && \
    . /root/.bashrc && \
    conda activate
RUN conda install -y click pip sqlite statsd
RUN pip install git+git://github.com/garyelephant/pygrok.git
COPY *.py .
RUN echo 'conda activate' >> /root/.bashrc
ENTRYPOINT [ "python", "log-parser.py"]

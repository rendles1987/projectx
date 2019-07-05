FROM ubuntu:18.04

RUN apt-get -y update \
    && apt-get -y install --no-install-recommends \
        # build-essential enables Makefile etc
        build-essential \
        python3-dev \
        python3-dev \
        python3-pip \
        git \
    && apt-get clean \
    && apt-get purge \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip3 install --upgrade pip
RUN pip install --upgrade pip
RUN pip3 install setuptools

COPY requirements.txt /work/requirements.txt
WORKDIR /work
RUN pip3 install -r requirements.txt
CMD python3 /work/main.py

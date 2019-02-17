FROM ubuntu:18.04

RUN apt-get -y update \
    && apt-get -y install --no-install-recommends \
        python3-dev \
        python3-pip \
        git \
    && apt-get clean \
    && apt-get purge \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip3 install --upgrade pip
RUN pip3 install setuptools

# https://stackoverflow.com/questions/34398632/docker-how-to-run-pip-requirements-txt-only-if-there-was-a-change
COPY requirements.txt /work/requirements.txt
WORKDIR /work
RUN pip install -r requirements.txt -U
COPY . /work
# continue as before...

# VOLUME /home/renierkramer/test123/:/work/data
VOLUME ~/test123/:/work/data
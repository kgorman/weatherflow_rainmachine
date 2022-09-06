FROM python:3.5

ADD . /weatherflow_rainmachine
WORKDIR /weatherflow_rainmachine
RUN apt-get update --allow-releaseinfo-change && apt-get install -y \
    openssl libssl-dev ssl-cert \
    iputils-ping python-dev build-essential 

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
CMD ["bash", "runit.sh"]
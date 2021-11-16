FROM python:3.10.0-bullseye
LABEL maintainer="paulopereira.dev"

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN useradd -m user

COPY ./import_users /home/user/app
COPY ./requirements.txt /home/user/requirements.txt

RUN python -m venv /home/user/venv && \
    apt-get update -y && \
    apt-get install -y build-essential unzip libaio-dev libpq-dev && \
    wget --quiet -O /tmp/instantclient-basiclite-linuxx64.zip https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip && \
    mkdir -p /home/user/driver/oracle_ctl/ && \
    unzip /tmp/instantclient-basiclite-linuxx64.zip -d /home/user/driver/oracle_ctl/ && \
    ln -s /home/user/driver/oracle_ctl/instantclient_21_4/libclntsh.so.11.1 libclntsh.so && \
    chown -R user:user /home/user

ENV LD_LIBRARY_PATH="/home/user/driver/oracle_ctl/instantclient_21_4:$LD_LIBRARY_PATH"
    
USER user

ENV PATH="/home/user/venv/bin:$PATH"

# install dependencies and commands
RUN pip install --upgrade pip && \
    pip install -r /home/user/requirements.txt

WORKDIR /home/user/app

ENTRYPOINT ["tail", "-f", "/dev/null"]
FROM registry.access.redhat.com/ubi10/ubi-minimal

LABEL name="DCI Analytics" version="0.0.1"
LABEL maintainer="DCI Team <distributed-ci@redhat.com>"

ENV LANG en_US.UTF-8

RUN mkdir /opt/dci-control-server
RUN mkdir /opt/dci-analytics
COPY . /opt/dci-analytics/
WORKDIR /opt/dci-analytics


RUN microdnf upgrade -y && \
  microdnf install -y python3 python3-pip python3-wheel libpq git && \
  microdnf install -y python3-devel make gcc gcc-c++ postgresql-devel diffutils findutils file vi && \
  pip install --no-cache-dir --upgrade pip && \
  pip install --no-cache-dir --requirement requirements.txt && \
  microdnf -y clean all

ENV PYTHONPATH /opt/dci-analytics:/opt/dci-control-server
EXPOSE 2345

CMD ["gunicorn", "wsgi:application", "--reload", "--access-logfile", "-", "--error-logfile", "-", "--log-level", "debug", "--bind", "0.0.0.0:2345"]

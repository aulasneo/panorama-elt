FROM python:3.8-slim-buster
LABEL Maintainer="andres@aulasneo.com"

RUN mkdir panorama_elt
COPY panorama_elt /panorama_elt
COPY panorama.py .

RUN mkdir config

COPY requirements.txt .

RUN pip install -r requirements.txt

# Note:  Building this on an alpine image because it requires a lot less patching
FROM python:3-alpine
# FROM opensciencegrid/osg-wn

RUN apk update && apk add build-base

# Version
ARG version

ADD . /gracc-reporting
WORKDIR /gracc-reporting

RUN pip install -r requirements.txt

RUN python setup.py install


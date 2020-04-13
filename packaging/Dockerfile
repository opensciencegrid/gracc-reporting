# Note:  Building this on an alpine image because it requires a lot less patching
FROM python:2.7-alpine
# FROM opensciencegrid/osg-wn

# Version
ARG version

ADD . /gracc-reporting
WORKDIR /gracc-reporting

RUN python setup.py install


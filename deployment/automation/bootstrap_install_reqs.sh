#!/bin/bash
set -xe
aws s3 cp s3://mentorhub-training-emr-scripts-bucket/scripts/requirements.txt /tmp/requirements.txt
sudo pip3 install -r /tmp/requirements.txt
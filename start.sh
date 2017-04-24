#!/bin/bash

export FLASK_APP=rentr.py
flask initdb
flask run &

#!/bin/bash

export FLASK_APP=rentr.py
flask initdb
flask run 
#flask run & > /dev/null 2>/dev/null

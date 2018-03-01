#!/bin/bash

export FLASK_APP=rentr.py
flask initdb
#flask run 
flask run & 1>/dev/null 2>/dev/null

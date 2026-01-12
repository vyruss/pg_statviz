#!/bin/bash
flake8 pg_statviz.py modules libs tests --count --ignore F722,W503 --max-line-length=79 --statistics

#!/bin/sh

pkill -9 "dkvsrv" || true #CI runner automatically will kill background process.

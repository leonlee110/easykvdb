#!/bin/sh

pkill raft
rm -rf bin
go build -o bin/raftrocks
cp start.sh bin/
cd bin
sh start.sh

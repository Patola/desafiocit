#!/bin/zsh - 
#===============================================================================
#
#          FILE: start.sh
# 
#         USAGE: ./start.sh 
# 
#   DESCRIPTION: 
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Cláudio "Patola" Sampaio (Patola), patola@gmail.comm
#  ORGANIZATION: MakerLinux
#       CREATED: 15/03/2018 01:55:04 -03
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error
export FLASK_APP=desafiocit.py
python -m flask run --host=0.0.0.0


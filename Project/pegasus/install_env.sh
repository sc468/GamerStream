#!/bin/bash

# Copyright 2015 Insight Data Science
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

sudo add-apt-repository ppa:openjdk-r/ppa -y

sudo apt-get update

## old
#sudo apt-get --yes --force-yes install ssh rsync openjdk-8-jdk scala python-dev python-pip python-numpy python-scipy python-pandas gfortran git supervisor ruby

## new, removed python part
sudo apt-get --yes --force-yes install ssh rsync openjdk-8-jdk scala gfortran git supervisor ruby

## install anaconda python3
cd ~
# You can change what anaconda version you want at 
# https://repo.continuum.io/archive/
wget https://repo.continuum.io/archive/Anaconda3-4.4.0-Linux-x86_64.sh
bash Anaconda3-4.4.0-Linux-x86_64.sh -b -p ~/anaconda3
rm   Anaconda3-4.4.0-Linux-x86_64.sh
echo 'export PATH="$(pwd $HOME)/anaconda3/bin:$PATH"' >> ~/.bashrc 
# Refresh basically
source ~/.bashrc
conda update conda
conda update anaconda


# get sbt repository
wget https://dl.bintray.com/sbt/debian/sbt-0.13.7.deb -P ~/Downloads
sudo dpkg -i ~/Downloads/sbt-*

# get maven3 repository
sudo apt-get purge maven maven2 maven3
sudo apt-add-repository -y ppa:andrei-pozolotin/maven3
sudo apt-get update
sudo apt-get --yes --force-yes install maven3

sudo update-java-alternatives -s java-1.8.0-openjdk-amd64

## old
#sudo pip install nose seaborn boto scikit-learn "ipython[notebook]"

# new with python3
conda install nose seaborn boto scikit-learn
pip install rethinkdb
pip install cassandra-driver


if ! grep "export JAVA_HOME" ~/.profile; then
  echo -e "\nexport JAVA_HOME=/usr" | cat >> ~/.profile
  echo -e "export PATH=\$PATH:\$JAVA_HOME/bin" | cat >> ~/.profile
fi



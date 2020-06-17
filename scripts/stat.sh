#!/bin/bash
# Template Format
#
# echo -----<Section Title>-----
# echo <Feature>: $(<Command>)
# echo <Feature>: $(<Command>)
# echo -----<Next Section Title>-----
# 
# Make sure only commands with single line outputs are used
# Make sure 5 hyphens are used before and after a title

echo -----Host-----
echo Uname: $(uname -a)

echo -----Homedir-----
echo HMD: $HOME

echo -----TEMPdir-----
echo TMP: $TMPDIR

echo -----Singularity test-----
echo Output: $(singularity exec -B /cvmfs:/cvmfs /cvmfs/alice.cern.ch/containers/fs/singularity/centos7 /bin/echo SUPPORTED)

echo -----Singularity config-----
echo Loop devices: $(cat /etc/singularity/singularity.conf | grep "loop devices")
echo Overlay: $(cat /etc/singularity/singularity.conf | grep "enable overlay" | head -1) 
echo Underlay: $(cat /etc/singularity/singularity.conf | grep "enable underlay")

echo -----Container?-----
echo Container Enabled?: $(ls / | grep workdir)

echo -----User namespaces?-----
echo Max namespaces: $(cat /proc/sys/user/max_user_namespaces)

sleep 60

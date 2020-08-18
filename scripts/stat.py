#!/usr/bin/env python3
import os
from subprocess import Popen,PIPE
import time  
import zipfile

url = os.environ.get('APMON_CONFIG')
jobId = os.environ.get('ALIEN_PROC_ID')
hostname = os.environ.get('ALIEN_HOSTNAME')


monitors = {
    'Uname': 'echo $(uname -a)',
    'HMD': 'echo $HOME',
    'TMP': 'echo $TMPDIR',
    'Singularity': 'echo $(singularity exec -B /cvmfs:/cvmfs /cvmfs/alice.cern.ch/containers/fs/singularity/centos7 /bin/echo SUPPORTED)',
    'Loop devices': 'echo $(cat /etc/singularity/singularity.conf | grep "loop devices")',
    'Overlay': 'echo $(cat /etc/singularity/singularity.conf | grep "enable overlay" | head -1)',
    'Underlay': 'echo $(cat /etc/singularity/singularity.conf | grep "enable underlay")',
    'Container Enabled?': 'echo $(ls / | grep workdir])',
    'LHCB Benchmark': 'echo $(bash /cvmfs/alice.cern.ch/scripts/lhcbmarks.sh)',
    'Max namespaces': 'echo $(cat /proc/sys/user/max_user_namespaces)'
}

if __name__ == "__main__":

    with zipfile.ZipFile('apmon.zip', 'r') as zip_ref:
        zip_ref.extractall('./')

    import apmon

    # Initialize ApMon specifying that it should not send information about the system.
    # Note that in this case the background monitoring process isn't stopped, in case you may
    # want later to monitor a job.
    apm = apmon.ApMon((url,))
    apm.setLogLevel("NOTICE")
    apm.confCheck = False
    apm.enableBgMonitoring(False)
    apm.setMaxMsgRate(75)
    apm.setMaxMsgSize(500)

    if not apm.initializedOK():
        print("It seems that ApMon cannot read its configuration. Setting the default destination")
    else:
        print("ApMon initialized succesfully")

    # you can put as many pairs of parameter_name, parameter_value as you want
    # but be careful not to create packets longer than 8K.

    for key in monitors.keys():
        command = monitors[key]
        value = ''
        with Popen(command, stdout=PIPE, bufsize=1, universal_newlines=True, shell=True) as p:
            print (command)
            for line in p.stdout:
                value += line
            print (value)
        apm.sendParameters("SiteSonar", jobId + "_" + hostname , {str(key): str(value)})
        time.sleep(.005)
    apm.sendParameters("SiteSonar", jobId + "_" + hostname , {"state": "complete"})
    time.sleep(300)
    print("Job Completed")
    

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os, re
import commands
import math, time
from os import listdir
from os.path import isfile, join

print
print 'START'
print
########   YOU ONLY NEED TO FILL THE AREA BELOW   #########
########   customization  area #########
MaxInputPerJob = 1 # number files to be processed in a single job, take care to split your file so that you run on all files. The last job might be with smaller number of files (the ones that remain).
queue = "8nh" # give bsub queue -- 8nm (8 minutes), 1nh (1 hour), 8nh, 1nd (1day), 2nd, 1nw (1 week), 2nw
Batch_Run_WORKDIR = os.getcwd()
TotalJobsNum = 0

########   customization end   #########

Tag = "mc_v1"
Use_PATH_TEXT = True
Use_File_TEXT = False

Check = False
Run = True
Test = False
ReSubmiited = False

UseBatch = True
UseLocally = False
path = os.getcwd()

print
print 'do not worry about folder creation:'

if Run:
    if Test:
        os.system("rm -rf tmp/")
        os.system("rm -rf res/")

    if not os.path.isdir("./tmp"):
        print("Create tmp/")
        os.system("mkdir -p tmp/"+Tag+"/list/splited")
        os.system("mkdir -p tmp/"+Tag+"/list/Unsplitted")
    else:
        if os.path.isdir("./tmp/"+Tag):
            print("Please change Tag version")
            sys.exit(0)
        else:
            os.system("mkdir -p tmp/"+Tag+"/list/splited")
            os.system("mkdir -p tmp/"+Tag+"/list/Unsplitted")
        # os.system("rm -r tmp")

    if not os.path.isdir("./res"):
        print("Create res/")
        # os.system("rm -r res")
        os.system("mkdir -p res/"+Tag)
    else:
        if os.path.isdir("./res/"+Tag):
            print("Please change Tag version")
            sys.exit(0)
        else:
            os.system("mkdir -p res/"+Tag)

if Check:
    print

# print



if Use_PATH_TEXT:
    with open("path.txt") as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    content = [x.strip() for x in content]
    # print(content)

    for file_dir in content:
        files_paths = [f[13:-20] for f in listdir(file_dir) if isfile(join(file_dir, f))]
        files_paths_no_replica = list(set(files_paths))
    print(files_paths_no_replica)

    if Run:
        for run_number in files_paths_no_replica:
            print("ls -d -1 "+file_dir+"/*.* |grep "+run_number+" > tmp/"+Tag+"/list/Unsplitted/list-"+run_number+".txt")
            os.system("ls -d -1 "+file_dir+"/*.* |grep "+run_number+" > tmp/"+Tag+"/list/Unsplitted/list-"+run_number+".txt")
            num_lines = sum(1 for line in open('tmp/'+Tag+'/list/Unsplitted/list-'+run_number+'.txt'))

            # split the files with MaxInputPerJob
            interval = num_lines/MaxInputPerJob
            remain = num_lines % MaxInputPerJob
            if remain != 0:
                TotalJobsNum = TotalJobsNum + (interval+1)
            if remain == 0:
                TotalJobsNum = TotalJobsNum + (interval)

            print
            print("Num of file = {0}, MaxInputPerJob = {1}, Splited to {2} files".format(str(num_lines), str(MaxInputPerJob), str(interval+1)))
            print

            print(os.getcwd())
            ##split files

            print("interval {0}, remain {1}".format(str(interval), str(remain)))

            if interval == 0:
                print("sed '"+str(1+MaxInputPerJob*(interval))+","+str(MaxInputPerJob*(interval)+remain)+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(interval)+".txt")
                os.system("sed '"+str(1+MaxInputPerJob*(interval))+","+str(MaxInputPerJob*(interval)+remain)+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(interval)+".txt")
                os.system("mkdir -p res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(interval))
                os.system("ls")
                os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(interval))
                print("You're here, {0}".format(os.getcwd()))
                print("Submiited Jobs")

                with open('job-'+str(run_number)+'.sh', 'w') as fout:
                    fout.write("#!/bin/sh\n")
                    fout.write("echo\n")
                    fout.write("echo\n")
                    fout.write("echo 'START---------------'\n")
                    fout.write("echo 'WORKDIR ' ${PWD}\n")
                    fout.write("export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase\n")
                    fout.write("alias setupATLAS='source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh'\n")
                    fout.write("echo ${ATLAS_LOCAL_ROOT_BASE}\n")
                    fout.write("setupATLAS\n")
                    fout.write("lsetup asetup\n")
                    fout.write("cd "+str(Batch_Run_WORKDIR)+"\n")
                    fout.write("cd ../..\n")
                    fout.write("echo 'where ' ${PWD}\n")
                    fout.write("asetup --restore\n")
                    fout.write("cd build\n")
                    fout.write("source */setup.sh\n")
                    fout.write("cd ..\n")
                    fout.write("cd run/Batch/res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(interval)+"\n")
                    fout.write("echo 'where ' ${PWD}\n")
                    fout.write("INTEXTFILE=\""+Batch_Run_WORKDIR+"/tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(interval)+".txt\"\n")
                    # fout.write("echo $INTEXTFILE\n")
                    fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")

                    # fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';EVTMAX=100;doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")
                    fout.write("echo 'STOP---------------'\n")
                    fout.write("echo\n")
                    fout.write("echo\n")

                os.system("chmod 755 job-"+str(run_number)+".sh")
                if UseLocally:
                    os.system("sh job-"+str(run_number)+".sh")
                if UseBatch:
                    print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                    os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")

                with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(interval)+".txt") as f1:
                    content = f1.readlines()
                # you may also want to remove whitespace characters like `\n` at the end of each line
                SampleListToRun = [x.strip() for x in content]
                SampleListToRun = ", ".join(SampleListToRun)

                print "job nr " + SampleListToRun + " submitted" + " (1/1)"


                os.chdir("../../..")
            else:
                for split_num in range(interval+1):
                    # print(split_num)

                    if split_num == interval:
                        print("Spc : You're here, {0}".format(os.getcwd()))

                        if remain == 0:
                            continue
                        else:
                            print("sed '"+str(1+MaxInputPerJob*(split_num))+","+str(MaxInputPerJob*(split_num)+remain)+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt")
                            os.system("sed '"+str(1+MaxInputPerJob*(split_num))+","+str(MaxInputPerJob*(split_num)+remain)+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt")
                            os.system("mkdir -p res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                            os.system("ls")
                            os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                            print("You're here, {0}".format(os.getcwd()))
                            print("Submiited Jobs")

                            with open('job-'+str(run_number)+'.sh', 'w') as fout:
                                fout.write("#!/bin/sh\n")
                                fout.write("echo\n")
                                fout.write("echo\n")
                                fout.write("echo 'START---------------'\n")
                                fout.write("echo 'WORKDIR ' ${PWD}\n")
                                fout.write("export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase\n")
                                fout.write("alias setupATLAS='source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh'\n")
                                fout.write("echo ${ATLAS_LOCAL_ROOT_BASE}\n")
                                fout.write("setupATLAS\n")
                                fout.write("lsetup asetup\n")
                                fout.write("cd "+str(Batch_Run_WORKDIR)+"\n")
                                fout.write("cd ../..\n")
                                fout.write("echo 'where ' ${PWD}\n")
                                fout.write("asetup --restore\n")
                                fout.write("cd build\n")
                                fout.write("source */setup.sh\n")
                                fout.write("cd ..\n")
                                fout.write("cd run/Batch/res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num)+"\n")
                                fout.write("echo 'where ' ${PWD}\n")
                                fout.write("INTEXTFILE=\""+Batch_Run_WORKDIR+"/tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt\"\n")
                                # fout.write("echo $INTEXTFILE\n")
                                fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")

                                # fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';EVTMAX=100;doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")
                                # fout.write("cmsenv\n")
                                # fout.write("cmsRun "+ScriptName+" outputFile='res/"+OutputFileNames+"_"+str(x)+".root' inputFiles_clear inputFiles_load='tmp/"+str(x)+"/list.txt'\n")
                                fout.write("echo 'STOP---------------'\n")
                                fout.write("echo\n")
                                fout.write("echo\n")

                            os.system("chmod 755 job-"+str(run_number)+".sh")
                            if UseLocally:
                                os.system("sh job-"+str(run_number)+".sh")
                            if UseBatch:
                                print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")

                            with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt") as f1:
                                content = f1.readlines()
                            # you may also want to remove whitespace characters like `\n` at the end of each line
                            SampleListToRun = [x.strip() for x in content]
                            SampleListToRun = ", ".join(SampleListToRun)

                            print "job nr " + SampleListToRun + " submitted" + " ({0}/{1})".format(str(split_num+1),str(interval+1))

                            os.chdir("../../..")
                    else:
                        print("sed '"+str(1+MaxInputPerJob*(split_num))+","+str(MaxInputPerJob*(split_num+1))+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt")
                        os.system("sed '"+str(1+MaxInputPerJob*(split_num))+","+str(MaxInputPerJob*(split_num+1))+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt")
                        os.system("mkdir -p res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                        print("You're here, {0}".format(os.getcwd()))
                        os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                        print("You're here, {0}".format(os.getcwd()))
                        print("Submiited Jobs")

                        with open('job-'+str(run_number)+'.sh', 'w') as fout:
                            fout.write("#!/bin/sh\n")
                            fout.write("echo\n")
                            fout.write("echo\n")
                            fout.write("echo 'START---------------'\n")
                            fout.write("echo 'WORKDIR ' ${PWD}\n")
                            fout.write("export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase\n")
                            fout.write("alias setupATLAS='source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh'\n")
                            fout.write("echo ${ATLAS_LOCAL_ROOT_BASE}\n")
                            fout.write("setupATLAS\n")
                            fout.write("lsetup asetup\n")
                            fout.write("cd "+str(Batch_Run_WORKDIR)+"\n")
                            fout.write("cd ../..\n")
                            fout.write("echo 'where ' ${PWD}\n")
                            fout.write("asetup --restore\n")
                            fout.write("cd build\n")
                            fout.write("source */setup.sh\n")
                            fout.write("cd ..\n")
                            fout.write("cd run/Batch/res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num)+"\n")
                            fout.write("echo 'where ' ${PWD}\n")
                            fout.write("INTEXTFILE=\""+Batch_Run_WORKDIR+"/tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt\"\n")
                            # fout.write("echo $INTEXTFILE\n")
                            fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")

                            # fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';EVTMAX=100;doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")
                            # fout.write("cmsenv\n")
                            # fout.write("cmsRun "+ScriptName+" outputFile='res/"+OutputFileNames+"_"+str(x)+".root' inputFiles_clear inputFiles_load='tmp/"+str(x)+"/list.txt'\n")
                            fout.write("echo 'STOP---------------'\n")
                            fout.write("echo\n")
                            fout.write("echo\n")

                        os.system("chmod 755 job-"+str(run_number)+".sh")

                        if UseLocally:
                            os.system("sh job-"+str(run_number)+".sh")
                        if UseBatch:
                            print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                            os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")

                        with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt") as f1:
                            content = f1.readlines()
                        # you may also want to remove whitespace characters like `\n` at the end of each line
                        SampleListToRun = [x.strip() for x in content]
                        SampleListToRun = ", ".join(SampleListToRun)

                        print "job nr " + SampleListToRun + " submitted" + " ({0}/{1})".format(str(split_num+1),str(interval+1))

                        os.chdir("../../..")
    if Check:
        for run_number in files_paths_no_replica:
            num_lines = sum(1 for line in open('tmp/'+Tag+'/list/Unsplitted/list-'+run_number+'.txt'))

            # split the files with MaxInputPerJob
            interval = num_lines/MaxInputPerJob
            remain = num_lines % MaxInputPerJob

            if interval == 0:
                os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(interval))
                print("You're here, {0}".format(os.getcwd()))
                print("Check!")
                if isfile("log.txt"):
                    os.system('cat log.txt|tail -2 > check.txt')
                    with open("check.txt") as c:
                        check = c.readlines()
                    check = [x.strip().split() for x in check]
                    print(check)
                    if 'code' in check[0] and '"successful' in check[0] and 'run"' in check[0]:
                        print("YES")
                    else:
                        print("Please Rusubmit the jobs!")
                        if ReSubmiited:
                            if UseLocally:
                                os.system("sh job-"+str(run_number)+".sh")
                            if UseBatch:
                                print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt") as f1:
                                    content = f1.readlines()
                                # you may also want to remove whitespace characters like `\n` at the end of each line
                                SampleListToRun = [x.strip() for x in content]
                                SampleListToRun = ", ".join(SampleListToRun)

                                print "job nr " + SampleListToRun + " submitted" + " ({0}/{1})".format(str(split_num+1),str(interval+1))

                else:
                    print("No log file!!")
                os.chdir("../../..")
            else:
                for split_num in range(interval+1):
                    # print(split_num)

                    if split_num == interval:
                        if remain == 0:
                            continue
                        else:
                            os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                            print("You're here, {0}".format(os.getcwd()))
                            print("Check!")
                            if isfile("log.txt"):
                                os.system('cat log.txt|tail -2 > check.txt')
                                with open("check.txt") as c:
                                    check = c.readlines()
                                check = [x.strip().split() for x in check]
                                print(check)
                                if 'code' in check[0] and '"successful' in check[0] and 'run"' in check[0]:
                                    print("YES")
                                else:
                                    print("Please Rusubmit the jobs!")
                                    if ReSubmiited:
                                        if UseLocally:
                                            os.system("sh job-"+str(run_number)+".sh")
                                        if UseBatch:
                                            print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                            os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                            with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt") as f1:
                                                content = f1.readlines()
                                            # you may also want to remove whitespace characters like `\n` at the end of each line
                                            SampleListToRun = [x.strip() for x in content]
                                            SampleListToRun = ", ".join(SampleListToRun)

                                            print "job nr " + SampleListToRun + " submitted" + " ({0}/{1})".format(str(split_num+1),str(interval+1))
                            else:
                                print("No log file!!")
                            os.chdir("../../..")
                    else:
                        os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                        print("You're here, {0}".format(os.getcwd()))
                        print("Check!")
                        if isfile("log.txt"):
                            os.system('cat log.txt|tail -2 > check.txt')
                            with open("check.txt") as c:
                                check = c.readlines()
                            check = [x.strip().split() for x in check]
                            print(check)
                            if 'code' in check[0] and '"successful' in check[0] and 'run"' in check[0]:
                                print("YES")
                            else:
                                print("Please Rusubmit the jobs!")
                                if ReSubmiited:
                                    if UseLocally:
                                        os.system("sh job-"+str(run_number)+".sh")
                                    if UseBatch:
                                        print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                        os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                        with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt") as f1:
                                            content = f1.readlines()
                                        # you may also want to remove whitespace characters like `\n` at the end of each line
                                        SampleListToRun = [x.strip() for x in content]
                                        SampleListToRun = ", ".join(SampleListToRun)

                                        print "job nr " + SampleListToRun + " submitted" + " ({0}/{1})".format(str(split_num+1),str(interval+1))
                        else:
                            print("No log file!!")
                        os.chdir("../../..")



if Use_File_TEXT:

    with open("data16_13TeV.periodL.physics_Main.PhysCont.DAOD_HIGG3D1.grp16_v01_p3388.txt") as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    content = [x.strip()[66:-20] for x in content]
    content_no_replica = list(set(content))

    print(content_no_replica)

    if Run:
        for run_number in content_no_replica:
            print("cat data16_13TeV.periodL.physics_Main.PhysCont.DAOD_HIGG3D1.grp16_v01_p3388.txt |grep "+run_number+" > tmp/"+Tag+"/list/Unsplitted/list-"+run_number+".txt")
            os.system("cat data16_13TeV.periodL.physics_Main.PhysCont.DAOD_HIGG3D1.grp16_v01_p3388.txt |grep "+run_number+" > tmp/"+Tag+"/list/Unsplitted/list-"+run_number+".txt")
            num_lines = sum(1 for line in open('tmp/'+Tag+'/list/Unsplitted/list-'+run_number+'.txt'))

            # split the files with MaxInputPerJob
            interval = num_lines/MaxInputPerJob
            remain = num_lines % MaxInputPerJob

            if remain != 0:
                TotalJobsNum = TotalJobsNum + (interval+1)
            if remain == 0:
                TotalJobsNum = TotalJobsNum + (interval)

            print
            print("Num of file = {0}, MaxInputPerJob = {1}, Splited to {2} files".format(str(num_lines), str(MaxInputPerJob), str(interval+1)))
            print

            print(os.getcwd())
            ##split files

            print("interval {0}, remain {1}".format(str(interval), str(remain)))

            if interval == 0:
                print("sed '"+str(1+MaxInputPerJob*(interval))+","+str(MaxInputPerJob*(interval)+remain)+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(interval)+".txt")
                os.system("sed '"+str(1+MaxInputPerJob*(interval))+","+str(MaxInputPerJob*(interval)+remain)+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(interval)+".txt")
                os.system("mkdir -p res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(interval))
                os.system("ls")
                os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(interval))
                print("You're here, {0}".format(os.getcwd()))
                print("Submiited Jobs")

                with open('job-'+str(run_number)+'.sh', 'w') as fout:
                    fout.write("#!/bin/sh\n")
                    fout.write("echo\n")
                    fout.write("echo\n")
                    fout.write("echo 'START---------------'\n")
                    fout.write("echo 'WORKDIR ' ${PWD}\n")
                    fout.write("export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase\n")
                    fout.write("alias setupATLAS='source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh'\n")
                    fout.write("echo ${ATLAS_LOCAL_ROOT_BASE}\n")
                    fout.write("setupATLAS\n")
                    fout.write("lsetup asetup\n")
                    fout.write("cd "+str(Batch_Run_WORKDIR)+"\n")
                    fout.write("cd ../..\n")
                    fout.write("echo 'where ' ${PWD}\n")
                    fout.write("asetup --restore\n")
                    fout.write("cd build\n")
                    fout.write("source */setup.sh\n")
                    fout.write("cd ..\n")
                    fout.write("cd run/Batch/res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(interval)+"\n")
                    fout.write("echo 'where ' ${PWD}\n")
                    fout.write("INTEXTFILE=\""+Batch_Run_WORKDIR+"/tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(interval)+".txt\"\n")
                    # fout.write("echo $INTEXTFILE\n")
                    fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")

                    # fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';EVTMAX=100;doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")
                    # fout.write("cmsenv\n")
                    # fout.write("cmsRun "+ScriptName+" outputFile='res/"+OutputFileNames+"_"+str(x)+".root' inputFiles_clear inputFiles_load='tmp/"+str(x)+"/list.txt'\n")
                    fout.write("echo 'STOP---------------'\n")
                    fout.write("echo\n")
                    fout.write("echo\n")

                os.system("chmod 755 job-"+str(run_number)+".sh")

                if UseLocally:
                    os.system("sh job-"+str(run_number)+".sh")
                if UseBatch:
                    print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                    os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")

                with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(interval)+".txt") as f1:
                    content = f1.readlines()
                # you may also want to remove whitespace characters like `\n` at the end of each line
                SampleListToRun = [x.strip() for x in content]
                SampleListToRun = ", ".join(SampleListToRun)

                print "job nr " + SampleListToRun + " submitted" + " (1/1)"


                os.chdir("../../..")
            else:
                for split_num in range(interval+1):
                    # print(split_num)

                    if split_num == interval:
                        print("Spc : You're here, {0}".format(os.getcwd()))

                        if remain == 0:
                            continue
                        else:
                            print("sed '"+str(1+MaxInputPerJob*(split_num))+","+str(MaxInputPerJob*(split_num)+remain)+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt")
                            os.system("sed '"+str(1+MaxInputPerJob*(split_num))+","+str(MaxInputPerJob*(split_num)+remain)+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt")
                            os.system("mkdir -p res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                            os.system("ls")
                            os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                            print("You're here, {0}".format(os.getcwd()))
                            print("Submiited Jobs")

                            with open('job-'+str(run_number)+'.sh', 'w') as fout:
                                fout.write("#!/bin/sh\n")
                                fout.write("echo\n")
                                fout.write("echo\n")
                                fout.write("echo 'START---------------'\n")
                                fout.write("echo 'WORKDIR ' ${PWD}\n")
                                fout.write("export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase\n")
                                fout.write("alias setupATLAS='source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh'\n")
                                fout.write("echo ${ATLAS_LOCAL_ROOT_BASE}\n")
                                fout.write("setupATLAS\n")
                                fout.write("lsetup asetup\n")
                                fout.write("cd "+str(Batch_Run_WORKDIR)+"\n")
                                fout.write("cd ../..\n")
                                fout.write("echo 'where ' ${PWD}\n")
                                fout.write("asetup --restore\n")
                                fout.write("cd build\n")
                                fout.write("source */setup.sh\n")
                                fout.write("cd ..\n")
                                fout.write("cd run/Batch/res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num)+"\n")
                                fout.write("echo 'where ' ${PWD}\n")
                                fout.write("INTEXTFILE=\""+Batch_Run_WORKDIR+"/tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt\"\n")
                                # fout.write("echo $INTEXTFILE\n")
                                fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")

                                # fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';EVTMAX=100;doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")
                                # fout.write("cmsenv\n")
                                # fout.write("cmsRun "+ScriptName+" outputFile='res/"+OutputFileNames+"_"+str(x)+".root' inputFiles_clear inputFiles_load='tmp/"+str(x)+"/list.txt'\n")
                                fout.write("echo 'STOP---------------'\n")
                                fout.write("echo\n")
                                fout.write("echo\n")

                            os.system("chmod 755 job-"+str(run_number)+".sh")

                            if UseLocally:
                                os.system("sh job-"+str(run_number)+".sh")
                            if UseBatch:
                                print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")

                            with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt") as f1:
                                content = f1.readlines()
                            # you may also want to remove whitespace characters like `\n` at the end of each line
                            SampleListToRun = [x.strip() for x in content]
                            SampleListToRun = ", ".join(SampleListToRun)

                            print "job nr " + SampleListToRun + " submitted" + " ({0}/{1})".format(str(split_num+1),str(interval+1))

                            os.chdir("../../..")
                    else:
                        print("sed '"+str(1+MaxInputPerJob*(split_num))+","+str(MaxInputPerJob*(split_num+1))+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt")
                        os.system("sed '"+str(1+MaxInputPerJob*(split_num))+","+str(MaxInputPerJob*(split_num+1))+"!d' tmp/"+Tag+"/list/Unsplitted/list-"+str(run_number)+".txt > tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt")
                        os.system("mkdir -p res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                        print("You're here, {0}".format(os.getcwd()))
                        os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                        print("You're here, {0}".format(os.getcwd()))
                        print("Submiited Jobs")

                        with open('job-'+str(run_number)+'.sh', 'w') as fout:
                            fout.write("#!/bin/sh\n")
                            fout.write("echo\n")
                            fout.write("echo\n")
                            fout.write("echo 'START---------------'\n")
                            fout.write("echo 'WORKDIR ' ${PWD}\n")
                            fout.write("export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase\n")
                            fout.write("alias setupATLAS='source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh'\n")
                            fout.write("echo ${ATLAS_LOCAL_ROOT_BASE}\n")
                            fout.write("setupATLAS\n")
                            fout.write("lsetup asetup\n")
                            fout.write("cd "+str(Batch_Run_WORKDIR)+"\n")
                            fout.write("cd ../..\n")
                            fout.write("echo 'where ' ${PWD}\n")
                            fout.write("asetup --restore\n")
                            fout.write("cd build\n")
                            fout.write("source */setup.sh\n")
                            fout.write("cd ..\n")
                            fout.write("cd run/Batch/res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num)+"\n")
                            fout.write("echo 'where ' ${PWD}\n")
                            fout.write("INTEXTFILE=\""+Batch_Run_WORKDIR+"/tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt\"\n")
                            # fout.write("echo $INTEXTFILE\n")
                            fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")

                            # fout.write("athena -c \"INTEXTFILE=\'$INTEXTFILE\';EVTMAX=100;doEffiSystematics=False;doP4Systematics=False;do2Lep=True;writePAOD_2L=False;writePAOD_2LDF=True\" PhysicsxAODConfig/HWWAnalysis_topOptions.py 2>&1 | tee log.txt\n")
                            # fout.write("cmsenv\n")
                            # fout.write("cmsRun "+ScriptName+" outputFile='res/"+OutputFileNames+"_"+str(x)+".root' inputFiles_clear inputFiles_load='tmp/"+str(x)+"/list.txt'\n")
                            fout.write("echo 'STOP---------------'\n")
                            fout.write("echo\n")
                            fout.write("echo\n")

                        os.system("chmod 755 job-"+str(run_number)+".sh")
                        if UseLocally:
                            os.system("sh job-"+str(run_number)+".sh")
                        if UseBatch:
                            print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                            os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")

                        with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt") as f1:
                            content = f1.readlines()
                        # you may also want to remove whitespace characters like `\n` at the end of each line
                        SampleListToRun = [x.strip() for x in content]
                        SampleListToRun = ", ".join(SampleListToRun)

                        print "job nr " + SampleListToRun + " submitted" + " ({0}/{1})".format(str(split_num+1),str(interval+1))

                        os.chdir("../../..")


    if Check:
        for run_number in content_no_replica:
            num_lines = sum(1 for line in open('tmp/'+Tag+'/list/Unsplitted/list-'+run_number+'.txt'))

            # split the files with MaxInputPerJob
            interval = num_lines/MaxInputPerJob
            remain = num_lines % MaxInputPerJob

            if interval == 0:
                os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(interval))
                print("You're here, {0}".format(os.getcwd()))
                print("Check!")
                if isfile("log.txt"):
                    os.system('cat log.txt|tail -2 > check.txt')
                    with open("check.txt") as c:
                        check = c.readlines()
                    check = [x.strip().split() for x in check]
                    print(check)
                    if 'code' in check[0] and '"successful' in check[0] and 'run"' in check[0]:
                        print("YES")
                    else:
                        print("Please Rusubmit the jobs!")
                        if ReSubmiited:
                            if UseLocally:
                                os.system("sh job-"+str(run_number)+".sh")
                            if UseBatch:
                                print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")

                                with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(interval)+".txt") as f1:
                                    content = f1.readlines()
                                # you may also want to remove whitespace characters like `\n` at the end of each line
                                SampleListToRun = [x.strip() for x in content]
                                SampleListToRun = ", ".join(SampleListToRun)

                                print "job nr " + SampleListToRun + " submitted" + " (1/1)"
                else:
                    print("No log file!!")
                os.chdir("../../..")
            else:
                for split_num in range(interval+1):
                    # print(split_num)

                    if split_num == interval:
                        if remain == 0:
                            continue
                        else:
                            os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                            print("You're here, {0}".format(os.getcwd()))
                            print("Check!")
                            if isfile("log.txt"):
                                os.system('cat log.txt|tail -2 > check.txt')
                                with open("check.txt") as c:
                                    check = c.readlines()
                                check = [x.strip().split() for x in check]
                                print(check)
                                if 'code' in check[0] and '"successful' in check[0] and 'run"' in check[0]:
                                    print("YES")
                                else:
                                    print("Please Rusubmit the jobs!")
                                    if ReSubmiited:
                                        if UseLocally:
                                            os.system("sh job-"+str(run_number)+".sh")
                                        if UseBatch:
                                            print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                            os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                            with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt") as f1:
                                                content = f1.readlines()
                                            # you may also want to remove whitespace characters like `\n` at the end of each line
                                            SampleListToRun = [x.strip() for x in content]
                                            SampleListToRun = ", ".join(SampleListToRun)

                                            print "job nr " + SampleListToRun + " submitted" + " ({0}/{1})".format(str(split_num+1),str(interval+1))
                            else:
                                print("No log file!!")
                            os.chdir("../../..")
                    else:
                        os.chdir("res/"+Tag+"/unmerged_"+str(run_number)+"-"+str(split_num))
                        print("You're here, {0}".format(os.getcwd()))
                        print("Check!")
                        if isfile("log.txt"):
                            os.system('cat log.txt|tail -2 > check.txt')
                            with open("check.txt") as c:
                                check = c.readlines()
                            check = [x.strip().split() for x in check]
                            print(check)
                            if 'code' in check[0] and '"successful' in check[0] and 'run"' in check[0]:
                                print("YES")
                            else:
                                print("Please Rusubmit the jobs!")
                                if ReSubmiited:
                                    if UseLocally:
                                        os.system("sh job-"+str(run_number)+".sh")
                                    if UseBatch:
                                        print("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                        os.system("bsub -q "+queue+" -o logs job-"+str(run_number)+".sh")
                                        with open("../../../tmp/"+Tag+"/list/splited/list-"+str(run_number)+"-"+str(split_num)+".txt") as f1:
                                            content = f1.readlines()
                                        # you may also want to remove whitespace characters like `\n` at the end of each line
                                        SampleListToRun = [x.strip() for x in content]
                                        SampleListToRun = ", ".join(SampleListToRun)

                                        print "job nr " + SampleListToRun + " submitted" + " ({0}/{1})".format(str(split_num+1),str(interval+1))
                        else:
                            print("No log file!!")
                        os.chdir("../../..")





print
print "Submitting ", TotalJobsNum, " jobs:"
os.system("bjobs")
print
print 'END'
print

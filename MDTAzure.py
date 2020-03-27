# -*- coding: utf-8 -*-
"""
Created on Mon Mar 12 14:07:35 2018

@author: TOSHIBA
"""
import argparse
import os 
#import azure.storage.blob as azureblob 



parser = argparse.ArgumentParser() 
parser.add_argument('--filepath', required=True,
                    help='The path to the text file to process. The path'
                    'may include a compute node\'s environment'
                    'variables, such as'
                    '$AZ_BATCH_NODE_SHARED_DIR/filename.txt')

parser.add_argument('--storageaccount', required=True,
                    help='The name the Azure Storage account that owns the'
                    'blob storage container to which to upload'
                    'results.')

parser.add_argument('--storagecontainer', required=True,
                    help='The Azure Blob storage container to which to'
                    'upload results.')

parser.add_argument('--taskfile', required=True,
                    help='Name of task file.')

parser.add_argument('--subjectid', required=True,
                    help='The name of subject id.')#subject id dışarıdan gelecek

parser.add_argument('--accountkey', required=True,
                    help='The key for the storage account.')#account key

                    
parser.add_argument('--tasknumber', required=True,
                    help='The task name with number.')


args = parser.parse_args()
hcp_subject_id=args.subjectid


#hcp_subject_id=Subjectid

#var='azcopy --source https://mounted.blob.core.windows.net/output/req --destination /mnt/batch/tasks/workitems/YeditepeBMEJob/job-1/{}/wd/workingfolder/req --source-key {} --recursive'.format(args.tasknumber,args.accountkey)
#os.system(var)
####
#######################################################################################
"""
varForosSystem='azcopy --source https://mounted.blob.core.windows.net/output/{} --destination /mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/workingfolder --source-key {} --recursive'.format(hcp_subject_id,args.tasknumber,args.accountkey)
os.system(varForosSystem)

varForosSystem='azcopy --source https://mounted.blob.core.windows.net/output/req/install_SDK_prereq_ubuntu.sh --destination /mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/install_SDK_prereq_ubuntu.sh --source-key {}'.format(args.tasknumber,args.accountkey)
os.system(varForosSystem)
"""
###########################################################################################
from os import makedirs
from os.path import expanduser,join,isdir,exists


home='/mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/workingfolder'.format(args.tasknumber)    

#home=expanduser('~')

if not('hcp_subject_id' in vars()):
    hcp_subject_id = '100307'
    
if not('home' in vars()):
    home = expanduser('~')

datapath=join(home,hcp_subject_id) + '\\'

dname=join(home,hcp_subject_id)

fbdata = join(dname,'data.nii.gz')
fbval=join(dname,'bvals')
fbvec=join(dname,'bvecs')
fbmask=join(dname,'nodif_brain_mask.nii.gz')
fbstandard2acpc=join(dname,'standard2acpc_dc.nii.gz')
fbacpc_dc2standard=join(dname,'acpc_dc2standard.nii.gz')
fbNonLinReg=join(dname,'NonlinearRegJacobians.nii.gz')

#if not(exists(fbdata)):
    
# Import the SDK
import boto3
import uuid

# Do not hard code credentials
hcpclient = boto3.resource(
    's3',
    region_name='us-west-2',
    aws_access_key_id='**************************',
    aws_secret_access_key='******************************'
)

#for bucket in hcpclient.buckets.all():
#    print(bucket.name)

bucket_name = 'hcp-openaccess'.format(uuid.uuid4())
#print('Creating new bucket with name: {}'.format(bucket_name))

# Now, the bucket object
hcpbucket = hcpclient.Bucket(bucket_name)

hcp_data_file = 'HCP_1200/' + hcp_subject_id + '/T1w/Diffusion/data.nii.gz'
hcp_bvecs_file  = 'HCP_1200/' + hcp_subject_id + '/T1w/Diffusion/bvecs'
hcp_bvals_file  = 'HCP_1200/' + hcp_subject_id + '/T1w/Diffusion/bvals'
hcp_bmask_file  = 'HCP_1200/' + hcp_subject_id + '/T1w/Diffusion/nodif_brain_mask.nii.gz'
hcp_standard2acpc_file  = 'HCP_1200/' + hcp_subject_id + '/MNINonLinear/xfms/standard2acpc_dc.nii.gz'
hcp_acpc_dc2standard_file  = 'HCP_1200/' + hcp_subject_id + '/MNINonLinear/xfms/acpc_dc2standard.nii.gz'
hcp_NonLinRegJacobians_file  = 'HCP_1200/' + hcp_subject_id + '/MNINonLinear/xfms/NonlinearRegJacobians.nii.gz'

if not(isdir(dname)):
    makedirs(dname)

    for obj in hcpbucket.objects.filter(Prefix=hcp_data_file):
        print(obj)
        hcpbucket.download_file(obj.key,fbdata)
    
    for obj in hcpbucket.objects.filter(Prefix=hcp_bvecs_file):
        print(obj)
        hcpbucket.download_file(obj.key,fbvec)
    
    for obj in hcpbucket.objects.filter(Prefix=hcp_bvals_file):
        print(obj)
        hcpbucket.download_file(obj.key,fbval)
        
    for obj in hcpbucket.objects.filter(Prefix=hcp_bmask_file):
        print(obj)
        hcpbucket.download_file(obj.key,fbmask)
    
    for obj in hcpbucket.objects.filter(Prefix=hcp_standard2acpc_file):
        print(obj)
        hcpbucket.download_file(obj.key,fbstandard2acpc)
        
    for obj in hcpbucket.objects.filter(Prefix=hcp_acpc_dc2standard_file):
        print(obj)
        hcpbucket.download_file(obj.key,fbacpc_dc2standard)
        
    for obj in hcpbucket.objects.filter(Prefix=hcp_NonLinRegJacobians_file):
        print(obj)
        hcpbucket.download_file(obj.key,fbNonLinReg)

#else:
#print('HCP Data file is already available at '+fbdata+'. Skipping download.')


#CREATE NEW BVALS
import numpy as np



'''

WRITE YOUR CODE HERE 

'''
import time
import datetime
time.sleep(1)
###################################################
#History Log
timenow = str(datetime.datetime.now())
timenow = timenow[0:10] + '_' + timenow[11:13] + '_' + timenow[14:16]

source = '/mnt/batch/tasks/shared/{} '.format(args.taskfile)
dest = '/mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/workingfolder/{}'.format(args.tasknumber,hcp_subject_id)
varForossystem = 'cp ' + source + dest
os.system(varForossystem)

sourc = '/mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/workingfolder/{}/{} '.format(args.tasknumber,hcp_subject_id,args.taskfile)
desti = '/mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/workingfolder'.format(args.tasknumber)
varforsyst = 'cp ' + sourc + desti
os.system(varforsyst)

source = '/mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/workingfolder/{} '.format(args.tasknumber,args.taskfile)
desti  = '/mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/workingfolder/{}/{}'.format(args.tasknumber,hcp_subject_id,timenow + '_' +args.taskfile)
vardorsyst = 'mv ' + source + desti
os.system(vardorsyst)

varFroOsystem = 'echo "\n {} script was run in {} \n " >> '.format(timenow + '_' +args.taskfile,timenow) + '/mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/workingfolder/{}/historylog.txt'.format(args.tasknumber,hcp_subject_id)
os.system(varFroOsystem)
#sudo mdt-model-fit "NODDI" data.nii.gz bvecs.prtcl nodif_brain_mask.nii.gz 
#mdt-generate-protocol /mnt/batch/tasks/workitems/YeditepeBMEJob/job-1/tasknumber0/wd/workingfolder/100307/bvecs /mnt/batch/tasks/workitems/YeditepeBMEJob/job-1/tasknumber0/wd/workingfolder/100307/bvecs
##################################################################################################################################################
#Uploading output file
varForosSystem='yes | azcopy --source /mnt/batch/tasks/workitems/YeditepeBMEJob1/job-1/{}/wd/workingfolder --destination https://mounted.blob.core.windows.net/output/{} --dest-key {} --recursive'.format(args.tasknumber,hcp_subject_id,args.accountkey)
os.system(varForosSystem)


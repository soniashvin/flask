
import logging
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

# [START imports]
from flask import Flask, render_template, request
# [END imports]

# [START create_app]
app = Flask(__name__)
# [END create_app]
credentials = GoogleCredentials.get_application_default()
dataproc = build('dataproc', 'v1', credentials=credentials)

zone='us-central1-a'
project='my-bitnami-project-236405'
region='global'
cluster_name='cluster-e0a5'
bucket_name=''
filename='train_and_apply.py'
bucket_name='my-bitnami-project-236405'
def submit_pyspark_job(dataproc, project, region,
                       cluster_name, bucket_name, filename):
    """Submits the Pyspark job to the cluster, assuming `filename` has
    already been uploaded to `bucket_name`"""
    job_details = {
        'projectId': project,
        'job': {
            'placement': {
                'clusterName': cluster_name
            },
            'pysparkJob': {
                'mainPythonFileUri':'gs://{}/{}'.format(bucket_name, filename)
            }
        }
    }
    result = dataproc.projects().regions().jobs().submit(
        projectId=project,
        region=region,
        body=job_details).execute()
    job_id = result['reference']['jobId']
    print('Submitted job ID {}'.format(job_id))
    return job_id


# [START form]
@app.route('/form')
def form():
    return render_template('form.html')
# [END form]


# [START submitted]
@app.route('/submitted', methods=['POST'])
def submitted_form():
    name = request.form['name']
    email = request.form['email']
    site = request.form['site_url']
    comments = request.form['comments']
	sas=submit_pyspark_job(dataproc, project, region,cluster_name, bucket_name, filename)
	
    # [END submitted]
    # [START render_template]
    return render_template(
        'submitted_form.html',
        name=name,
        email=email,
        site=site,
        comments=comments
		ssa=sas)
    # [END render_template]


	
@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500
# [END app]

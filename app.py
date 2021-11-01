from flask import Flask, jsonify, request, abort
import boto3
from functools import wraps
import requests
import os
import json
import time
from dotenv import load_dotenv


load_dotenv()
app = Flask(__name__)
SECRET_KEY = os.getenv('SECRET_API_KEY')
region_name = os.getenv('AWS_REGION_NAME')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')


def delete_job(job_name, transcribe_client):
    """
    Deletes a transcription job. This also deletes the transcript associated with
    the job.

    :param job_name: The name of the job to delete.
    :param transcribe_client: The Boto3 Transcribe client.
    """
    transcribe_client.delete_transcription_job(TranscriptionJobName=job_name)


def transcribe_file(job_name, file_uri, applicantid, transcribe_client):
    transcribe_client.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': file_uri},
        MediaFormat='mp4',
        LanguageCode='en-US'
    )

    max_tries = 60
    while max_tries > 0:
        max_tries -= 1
        job = transcribe_client.get_transcription_job(
            TranscriptionJobName=job_name)
        job_status = job['TranscriptionJob']['TranscriptionJobStatus']
        if job_status in ['COMPLETED', 'FAILED']:
            print(f"Job {job_name} is {job_status}.")
            if job_status == 'COMPLETED':
                obj = requests.get(job['TranscriptionJob']['Transcript']
                                   ['TranscriptFileUri'], allow_redirects=True)._content
                return {'response': {'applicant id': applicantid, 'transcribtion': json.loads(obj.decode('utf8')), "status_code": 200}}

            else:
                return {"response": {"status_code": 400,  "message": "video can't be processed"}}
        time.sleep(1)


def require_appkey(view_function):
    @wraps(view_function)
    # the new, post-decoration function. Note *args and **kwargs here.
    def decorated_function(*args, **kwargs):
        if request.args.get("api-key") and request.args.get("api-key") == SECRET_KEY:
            return view_function(*args, **kwargs)
        else:
            abort(401)

    return decorated_function


@app.route('/transcribe', methods=["POST"])
@require_appkey
def usage_demo():

    # Getting Videos URLs
    video = request.get_json(force=True)['video']

    file_uri = 's3://velents-production' + video['video_url'].split('.com')[-1]
    
    applicant_id = video['applicant_id']

    transcribe_client = boto3.client('transcribe', region_name=region_name, aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key)

    try:
        res = transcribe_file('job', file_uri, applicant_id, transcribe_client)
    except:
        delete_job("job", transcribe_client)
        res = transcribe_file('job', file_uri, applicant_id, transcribe_client)
    delete_job("job", transcribe_client)
    return jsonify(res)


if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8888)

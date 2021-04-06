import functools
import json
import time
import traceback
from datetime import datetime, timedelta

import requests
import schedule
from bson.objectid import ObjectId
from pymongo import MongoClient
from pytz import timezone
from tzlocal import get_localzone

from config import uri, webhook_url

# <-- Date Time Format Handle --> #

format = "%H:%M:%S %Z%z"
now_utc = datetime.now(timezone('EST'))
now_local = now_utc.astimezone(get_localzone())
today = datetime.now()
yest, tomm = today - timedelta(days=1), today + timedelta(days=1)
ordered_stages = ['brainapi.receive', 'file.receive', 'algo.config', 'file.config', 'file.migrate', 'file.copy',
                  'data.extract', 'file.decrypt', 'file.transform', 'file.client', 'file.load', 'data.transform',
                  'pipeline.file', 'pipeline.algo', 'pipeline.dependency', 'report.file']

# <-- Filter to sort the Things as per Need from the Mongodb -->#

processing_pipeline_query = {
    "$and": [{"created.date": {"$gte": yest}}, {"created.date": {"$lt": tomm}}, {"$or": [{"status": "processing"}]}]}
failed_pipeline_query = {
    "$and": [{"created.date": {"$gte": yest}}, {"created.date": {"$lt": tomm}}, {"$or": [{"status": "error"}]}]}
clients = sorted(
    ["algo", "algomus", "aec", "catad", "crosby", "dadc", "default", "dis", "excell", "fox", "goldeneye", "joint-venture", "jointventure" ,"jv", "penske", "sphe",
     "uphe", "wbe", "whv-eu", "whveu", "diseu", "dis-eu", "target"], reverse=True)
filenames = sorted(
    ["GOLDENEYE_BATCH1_CORE_COMPUTE", "GOLDENEYE_UPHE_PA042_CORE_COMPUTE", "GOLDENEYE_UPHE_PA048_CORE_COMPUTE", "GOLDENEYE_UPHE_PA045_CORE_COMPUTE",
     "GOLDENEYE_DIS_PA061_CORE_COMPUTE",
     "GOLDENEYE_DIS_PA062_CORE_COMPUTE", "GOLDENEYE_JV_PA033_CORE_COMPUTE", "GOLDENEYE_UPHE_PA041_CORE_COMPUTE",
     "GOLDENEYE_UPHE_PA044_CORE_COMPUTE", "GOLDENEYE_SPHE_PA011_CORE_COMPUTE",
     "GOLDENEYE_JV_PA032_CORE_COMPUTE", "GOLDENEYE_SPHE_PA012_CORE_COMPUTE", "GOLDENEYE_UPHE_PA040_CORE_COMPUTE",
     "GOLDENEYE_JV_PA030_CORE_COMPUTE",
     "GOLDENEYE_DIS_PA060_CORE_COMPUTE", "GOLDENEYE_SPHE_PA014_CORE_COMPUTE", "GOLDENEYE_SPHE_PA015_CORE_COMPUTE",
     "GOLDENEYE_JV_PA031_CORE_COMPUTE",
     "GOLDENEYE_SPHE_PA016_CORE_COMPUTE", "GOLDENEYE_UPHE_PA043_CORE_COMPUTE", "GOLDENEYE_DIS_PA063_CORE_COMPUTE",
     "GOLDENEYE_BATCH4_CORE_COMPUTE"], reverse=True)
core_compute_list = []
for f in filenames:
    batch = 'DEPENDENCY_' + f
    core_compute_list.append(batch)
try:
    db = MongoClient(uri).get_database()
    collection = db['ProcessQueue']
    meta_collection = db['ProcessQueueMeta']
except Exception as e:
    print(e)


def get_filename(rec):
    if rec.get('configuration'):
        file_name = rec.get('configuration').get('fileName')
    else:
        file_name = rec.get("receive").get('fileName')
    if file_name:
        file_name = file_name.split('.')[0] if "." in file_name else file_name
    else:
        file_name = None
    return file_name


# <-- Function For Checking the Processing And  GE Core Compute Status Pipeline .

def check_processing_pipeline_status():
    try:
        global current_stage
        results = collection.find(processing_pipeline_query)
        processing_pipeline, core_compute, time_taken_pipeline = {}, {}, {}
        process_queue_msg, core_compute_msg, time_taken_pipeline_msg = "", "", ""
        for rec in results:
            origin = [client.upper() for client in clients if (client in rec["origin"].lower())][0]
            if origin == 'GOLDENEYE':
                origin = 'GE'
            if origin == "ALGO":
                origin = 'TARGET'
            if origin == 'DIS':
                origin = 'FOX'
            if origin == "JOINT-VENTURE" or origin == "JOINTVENTURE":
                origin = "SDS"
            
            current_stage = rec['stage'].split('.')
            if current_stage[0] in clients and rec['stage'] != 'algo.config':
                current_stage = "{}.{}".format(current_stage[1], current_stage[2].split(':')[0])
            else:
                current_stage = "{}.{}".format(current_stage[0], current_stage[1].split(':')[0])
            if not processing_pipeline.get(origin):
                processing_pipeline[origin] = {current_stage: 1}
            else:
                processing_pipeline[origin][current_stage] = processing_pipeline[origin][current_stage] + 1 if \
                    processing_pipeline[origin].get(current_stage) else 1
            object_id = str(rec["_id"])
            batch_id = str(rec["batchId"])
            start_time = rec['created']['date']
            stage = rec['stage'].split('.')
            stage = "{}.{}".format(stage[0], stage[1].split(':')[0])
            time_taken = datetime.utcnow() - start_time
            hours_taken = (time_taken.total_seconds() / 60) / 60
            file_name = get_filename(rec)
            if file_name:
                if hours_taken > 0.68:
                    h = int(time_taken.total_seconds())
                    m, s = divmod(h, 60)
                    h, m = divmod(m, 60)
                    hours_taken = ("%d:%02d:%02d" % (h, m, s))
                    time_taken_pipeline[origin, file_name, object_id] = hours_taken
                if (file_name in core_compute_list) or (file_name in filenames):
                    f_name = file_name.split('DEPENDENCY_')
                    if len(f_name) > 1:
                        core_compute[f_name[1]] = [object_id, rec['status'], stage]
                    else:
                        core_compute[f_name[0]] = [object_id, rec['status'], stage]
            # else:
            #     print("file name not found for Processing Pipeline", str(rec["_id"]))
        print(f"Process Queue:\n")
        if len(processing_pipeline) > 0:
            for client, stages in processing_pipeline.items():
                total, processing_stages = 0, ""
                for ordered_stage in ordered_stages:
                    if processing_pipeline[client].get(ordered_stage):
                        count = processing_pipeline[client][ordered_stage]
                        total = total + count
                        processing_stages = processing_stages + "{}-{} ".format(ordered_stage, count)
                process_queue_msg = process_queue_msg + "{:>7} : {:>4} processing ({:>5}) \n".format(client, total,
                                                                                                     processing_stages)
        else:
            process_queue_msg = "Processing : 0"
        print(process_queue_msg)

        print("\nGE Core Compute Status:\n")
        if len(core_compute) > 0:
            for key, value in core_compute.items():
                core_compute_msg = core_compute_msg + "{} - {} - {} - {}\n".format(key, core_compute[key][0],
                                                                                   core_compute[key][2],
                                                                                   core_compute[key][1])
        else:
            core_compute_msg = "None\n"
        print(core_compute_msg)

        if len(time_taken_pipeline) > 0:
            print('\nUnusual Long running Pipelines :\n')
            for q, interval_time in time_taken_pipeline.items():
                next_val = "{:>6} - {:>6} - {:>6} - {:>6} - {:>6} - {:>6} - {:>6}\n".format(q[0], q[1], q[2],
                                                                                            'processing', current_stage,
                                                                                            'Time', interval_time)
                time_taken_pipeline_msg = time_taken_pipeline_msg + next_val
            print(time_taken_pipeline_msg)
        else:
            time_taken_pipeline_msg = "None"
        msgs = {"process_queue_msg": process_queue_msg, "core_compute_msg": core_compute_msg,
                "time_taken_pipeline_msg": time_taken_pipeline_msg}
        return msgs
    except Exception as exc:
        print(traceback.print_exc())


# <-- Function For Checking the Failed Status Pipeline {Coded By : Anup A .Ingale } --> #

def check_failed_pipeline_status():
    try:
        global double_post_message, error
        results = collection.find(failed_pipeline_query)
        double_post_list, has_error, failed_pipelines = {}, True, ""
        for r in results:
            if r["receive"].get('fileName') not in ['MAINTENANCE_DAILY_VACUUM_REDSHIFT_CORE.trigger',
                                                    'MAINTENANCE_WEEKLY_VACUUM_REDSHIFT_CORE.trigger']:
                if has_error:
                    print("\nFailed Pipeline Check Status:\n")
                    has_error = False
                origin = [client.upper() for client in clients if (client in r["origin"].lower())][0]
                if origin == 'GOLDENEYE':
                    origin = 'GE'
                if origin == "ALGO":
                    origin = 'TARGET'
                if origin == 'DIS':
                    origin = 'FOX'
                if origin == "JOINT-VENTURE"  or origin == "JOINTVENTURE":
                    origin = "SDS"
                file_name = get_filename(r)
                if file_name:
                    stage = r["stage"].replace("redshift", '').replace("error", '').replace(":", '').replace("transform.",
                                                                                                             "transform")
                    object_id = str(r["_id"])
                    batch_id = str(r["batchId"])
                    meta_query = {"processQueueId": ObjectId(r["_id"])}
                    error_details = meta_collection.find(meta_query).sort("date")
                    for e in error_details:
                        try:
                            error = e["messages"][-1]
                        except KeyError:
                            error = e["message"]
                        date = str(e["date"])
                    message = str("Reject: S3 validation failed. Error - NotFound: null")
                    if str(error) == message:
                        object_id = str(r["_id"])
                        request_amazon_id = r['receive']['event']['Records'][0]['responseElements']['x-amz-request-id']
                        double_post = collection.find(
                            {"receive.event.Records.0.responseElements.x-amz-request-id": request_amazon_id})
                        list_check = []
                        for rec in double_post:
                            list_check.append(rec['status'])
                        if 'complete' in list_check and 'error' in list_check:
                            double_post_message = '( Confirm Double Lambda Post )'
                            double_post_list[id] = r['created']['date']
                            failed_pipelines = failed_pipelines + "{} - {} - Failed on - {} - {} - Process-ID:{} - Batch-ID:{} - {} \n\n".format(origin,
                                                                                                                     file_name,
                                                                                                                     stage,
                                                                                                                     str(
                                                                                                                         error),
                                                                                                                     object_id,
                                                                                                                     batch_id,
                                                                                                                     double_post_message)
                        elif 'error' in list_check:
                            failed_pipelines = failed_pipelines + "{} - {} - Failed on - {} - {} - Process-ID:{} - Batch-ID:{}\n\n".format(origin,
                                                                                                                file_name,
                                                                                                                stage,
                                                                                                                str(error),
                                                                                                                object_id,
                                                                                                                batch_id)
                    elif str(error) != message:
                        failed_pipelines = failed_pipelines + "{} - {} - Failed on - {} - {} - Process-ID:{} - Batch-ID:{}\n\n".format(origin,
                                                                                                            file_name,
                                                                                                            stage,
                                                                                                            str(error), object_id,
                                                                                                            batch_id)
                else:
                    print("Unable to find file Name for failed pipeline ", str(r["_id"]))
        if failed_pipelines == "":
            print("\nFailed pipeline Check status:")
            failed_pipelines = "None\n"
        print(failed_pipelines)
        return failed_pipelines
    except Exception as e:
        traceback.print_exc()


# <-- Function to send the response to slack Coded by { Adinath Gore and Anup Ingale } --> #

def message_sender(pq):
    try:
        if pq:
            process_queue = check_processing_pipeline_status()
            msg = "*Process Queue:*```{}```\n*GE Core Compute Status:*```{}```\n* Unusual Long running " \
                  "Pipelines:*```{}```\n".format(process_queue["process_queue_msg"], process_queue["core_compute_msg"],
                                                 process_queue["time_taken_pipeline_msg"])
        else:
            failed_pipelines = check_failed_pipeline_status()
            msg = "*Failed pipeline Check status::*```{}```".format(failed_pipelines)
        message = {"text": msg}
        is_sent = requests.post(webhook_url, json.dumps(message))
        if is_sent.status_code == 200:
            print("Message sent successfully !!")
        # else:
        #     print("message sending failed with error code: ", is_sent.status_code)
    except Exception as e:
        print(traceback.print_exc())


# <-- Schedule to set runing the Script Fuction at certain Intervals -->#

schedule.every(30).minutes.do(functools.partial(message_sender, True))
schedule.every(31).minutes.do(functools.partial(message_sender, False))
check_processing_pipeline_status()
check_failed_pipeline_status()
schedule.every(120).seconds.do(check_processing_pipeline_status)
schedule.every(122).seconds.do(check_failed_pipeline_status)

while True:
    schedule.run_pending()
    time.sleep(1)
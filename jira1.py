#!p/usr/bin/env python3
''' Loads the data from jira into the provided database for analysis '''

import argparse
import gitlab
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, exc
import sys
import datetime
import dateutil
import pytz
import numpy as np
import json
import requests
import adal
import re

parser = argparse.ArgumentParser(description="Generate csv report of commit lifetime for merge requests",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--gitlab_url', default='https://git.jpg.com', help='URL of gitlab instance to query')
parser.add_argument('--jira_token', required=True, help='Access token for jira, requires read access.')
parser.add_argument('--powerbi_url', help='URL to push data to a Power BI streaming dataset. Skipped if not provided.')
parser.add_argument('--after', default="2021-02-01T00:00:00.0000Z", help='Retrieve merge requests after this ISO 8601 date.')
parser.add_argument('--before', default=None, help='Limit merge requests to before this ISO 8601 date. Data is still retrieved to present to not miss any merged before this but updated after.')
parser.add_argument('--debug', action='store_true', help='Enable debug log output')
parser.add_argument('--last_day', action ='store_true', help='Generate report for the last full day.')
parser.add_argument('--clear_powerbi', action='store_true', help='Clears out all existing powerbi data first.')

# framework for updating the powerBI spreadsheet daily
args = parser.parse_args()
after = pd.to_datetime(args.after).astimezone(pytz.utc)
before = None
if args.before:
    before = pd.to_datetime(args.before).astimezone(pytz.utc)
if args.last_day:
    lastDay = datetime.datetime.utcnow().astimezone(pytz.utc) - datetime.timedelta(days=1)
    after = lastDay.replace(hour=0, minute=0, second=0, microsecond=0)
    before = after + dateutil.relativedelta.relativedelta(days=1)
target_branches = args.target_branches.split(',')

# This is the workspace id for the Engineering workspace. If you need a different workspace, browse to it
# and look at the url.  The id will be right after /groups/ (for the rest api, workspace == groups)
workspace_id = 'd0a84180-12dc-45cf-87c6-a2af23c30dde'
if args.clear_powerbi:
    # Attempt to parse out dataset id from the push url.
    # 'https://api.powerbi.com/beta/<some unique id?>/datasets/<the dataset id>/rows?key=<some key for pushing>'
    id_search = re.search('api\.powerbi\.com/beta/[a-z0-9-]+/datasets/([a-z0-9-]+)/.*', args.powerbi_url)
    if not id_search:
        print('Failed to parse dataset id from the PowerBI Push URL, are you sure the URL is valid?')
        sys.exit(1)
    dataset_id = id_search.group(1)

print('Retrieving merge requests from {} to {} into branches {}'.format(after, before, target_branches))
with gitlab.Gitlab(args.gitlab_url, private_token=args.gitlab_token) as gitlab_conn:
    projects = gitlab_conn.projects.list(as_list=False, page_size=100)
    df = pd.DataFrame(
        columns=['Group', 'Project', 'Commit_ID', 'Committed_date', 'Created_at', 'lifetime_hours', 'Target_Branch', 'Source_Branch'])
    daily = {}
    for project in projects:
        # get the merge request via iid
        try:
            print('<------------------------------------------>')
            print('Group: {} Project: {}'.format(project.namespace['full_path'], project.name))
            mrs = project.mergerequests.list(
                as_list=False, page_size=100, state='merged', updated_after=after.strftime('%Y-%m-%dT%H:%M:%S.0000Z'))
            for mr in mrs:
                # Skip merge requests that are unmerged, or not in our target branches.
                if mr.committed_date is None or mr.target_branch not in target_branches:
                    continue
                committed_date_utc = pd.to_datetime(
                    mr.committed_date).astimezone(pytz.utc)
                # Additionally ones merged before or after our target ranges.
                if committed_date_utc < after or (before and committed_date_utc > before):
                    continue
                # Create daily stats array if it doesn't exist yet.
                day = committed_date_utc.strftime('%Y-%m-%d')
                if day not in daily:
                    daily[day] = { 'total': [], 'group': {}}
                print('========================================================')
                print('MR IID:{} Mr_created_at:{} Committed_date:{} Target_branch: {} Source_branch: {}'.format(
                    mr.iid, mr.created_at, mr.committed_date, mr.target_branch, mr.source_branch))
                for commit in mr.commits():
                    if args.debug:
                        print('Commit: - {}'.format(commit))
                    commit_created_at_utc = pd.to_datetime(
                        commit.created_at).astimezone(pytz.utc)
                    commit_life = committed_date_utc - commit_created_at_utc
                    commit_life_hours = commit_life.total_seconds() / 3600
                    # Add to daily stats and csv output
                    daily[day]['total'].append(commit_life_hours)
                    group = project.namespace['full_path']
                    if group not in daily[day]['group']:
                        daily[day]['group'][group] = []
                    daily[day]['group'][group].append(commit_life_hours)
                    df = df.append({'Group': group, 'Project': project.name,
                                    'Commit_ID': commit.id, 'committed_date': committed_date_utc,
                                    'Created_at': commit_created_at_utc, 'lifetime_hours': commit_life_hours,
                                    'Target_Branch': mr.target_branch, 'Source_Branch': mr.source_branch
                                    }, ignore_index=True)
        except gitlab.exceptions.GitlabMRForbiddenError as e:
            print('Forbidden MRs in project: {}'.format(project.name))
            print(e)
            continue
        except gitlab.exceptions.GitlabListError as e:
            print(e)
            continue
    df.to_csv('commit_2.csv')

    def pushToPowerBI(data):
        if not args.powerbi_url:
            return # No url to push to.
        #print('Posting\n{}\n\nto: {}'.format(payload, args.powerbi_url))
        r = requests.post(args.powerbi_url, json=data)
        if not r:
            print('Push to powerbi failed with status code {}:\n{}'.format(r.status_code, r.text))
        else:
            print('Push to powerbi succeeded.')
    def clearPowerBI():
        if not args.powerbi_url or not args.clear_powerbi:
            return
        
        # Authenticate with AD.
        authority_url = 'https://login.microsoftonline.com/30834dbb-f907-42d3-9bbb-0c00c6094c93'
        context = adal.AuthenticationContext(
            authority_url,
            validate_authority=True
        )
        # figure out why token is not accepted
        token = context.acquire_token_with_client_credentials(
            resource='https://analysis.windows.net/powerbi/api',
            client_id='55b00e2a-928b-4c86-8636-62fe0f71b2d5',
            client_secret='Po_dR_5jRG.g99OXrFRpG1ohYW8Q6-Mt_I'
        )

        if 'accessToken' not in token:
            print('Failed to get Power BI Access token.')
            sys.exit(1)

        access_token = token['accessToken']
        header = {'Content-Type':'application/json','Authorization': f'Bearer {access_token}'}

        r = requests.delete(headers=header, url='https://api.powerbi.com/v1.0/myorg/groups/{}/datasets/{}/tables/RealTimeData/rows'.format(workspace_id, dataset_id))
        if not r:
            print('PowerBI clear failed with status code {}:\n{}'.format(r.status_code, r.text))
        else:
            print('Clearing of powerbi succeeded.')


#fix daily keys and use timedelta later
#also add weekly, monthly, and yearly using the same code structure
    powerbi_rows = []
    numbers = list(range(1,365))
    for day in sorted(numbers, daily = numbers.__getitem__ ()):
        median = np.median(day[day]['total'])
        mean = np.mean(day[day]['total'])
        print("{}\nTotal Median: {} Average: {}".format(day, median, mean))
        powerbi_rows.append({
                'day': day,
                'group': 'total',
                'medianLifetime': median,
                'meanLifetime': mean
            })
        for group in sorted(day[day]['group'].keys()):
            median = np.median(day[day]['group'][group])
            mean = np.mean(day[day]['group'][group])
            mode = np.mode(day[day]['group'][group])
            print("    {} Median: {} Mode: {} Average: {}".format(group, median, mode, mean ))
            powerbi_rows.append({
                    'day': day,
                    'group': group,
                    'medianLifetime': median,
                    'meanLifetime': mean
                })
    clearPowerBI()
    pushToPowerBI(powerbi_rows)

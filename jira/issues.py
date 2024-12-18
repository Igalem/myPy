from jira import JIRA
from datetime import datetime
import os
import csv

jira_domain = os.environ.get('P_JIRA_DOMAIN')
jira_username = os.environ.get('P_JIRA_USERNAME')
jira_token = os.environ.get('P_JIRA_DOMAIN')

options = {
 'server': jira_domain
}

jira = JIRA(
                    jira_domain,
                    basic_auth=(jira_username, jira_token)
                )


## ------- tickets: 
ticket = 'XXXX-9999'
issue = jira.issue(ticket)
summary = issue.fields.summary
print('ticket: ', ticket, summary)

projects = jira.projects()
for p in projects:
	print(p.id, p.key, p.name)

# # Search returns first 50 results, `maxResults` must be set to exceed this
issues_in_proj = jira.search_issues('project=XXXX order by ',startAt=0,maxResults=100)
issues_in_proj = jira.search_issues('project=XXXX and updated > today ',startAt=0,maxResults=100)
issues_in_proj = jira.search_issues('created > "2022-08-01 08:00"',startAt=0,maxResults=100)

### ===================================================

#today = datetime.strftime(datetime.today(), '%Y-%m-%d')
today = '2023-01-16'

## query all issues by project
#jql = 'project=XXXX and created > "{today}"'.format(today=today) 

## query all issues greather then Today
# jql = 'created > "{today}"'.format(today=today)

## query by issue
ticket = 'XXXX-9999'
issue = jira.issue(ticket)
jql = f'issue = "{issue}"'

maxResults = 100

startRun = datetime.today()
startAt = 0
jira_issues = []
values = []
data = []

fields = {
			'created' : None,  ## Created
			'updated' : None,  ## Updated
			'description': None,
			'issuetype' : 'name',  ## Issue Type
			'status' : 'name', ## Issue Status
			'project' : 'name',   ## Project
			'customfield_10717' : 'value',  ## Kenshoo Team
			'reporter' : 'displayName',  ## Reporter
			'creator' : 'displayName',  ## Creator
			'assignee' : 'displayName',  ## Assignee
			'customfield_10007' : 'name',  ## Sprint
			'customfield_15476' : 'value',  ## Eenvironment
			'priority' : 'name',  ## Priority
			'labels' : None,
			'comment' : 'total',
			'watches' : 'watchCount',
			'votes' : 'votes',
			'timeestimate' : None,
			'timetracking' : 'timeSpentSeconds', ## Total spent (in seconds)
			'progress' : 'total',  ## Total Progress (in seconds)
			'subtasks' : None,
			# 'issuelinks' : COUNT,
			'components' : None
		  }

while(True):
	print(startAt)
	jira_search_issues = jira.search_issues(jql, startAt=startAt, maxResults=maxResults)
	if not jira_search_issues:
		break
	for jira_search_issue in jira_search_issues:
		jira_issues.append(jira_search_issue.key)
	startAt+=maxResults

for jira_issue in jira_issues:
	issue = jira.issue(jira_issue)
	issue_key = issue.key
	print(issue_key)
	issue_fields = issue.raw['fields']
	row = []
	row.append(issue_key)
	for field in fields:
		if not fields[field]:
			try:
				value = issue_fields[field]
			except:
				value = ''
		else:
			try:
				value = issue_fields[field][fields[field]]
			except:
				value = ''
		row.append(value)
	data.append(row)	

endRun = datetime.today()
eta = endRun - startRun

### =======================================================

headers = [head for head in fields]
with open('/xxxx/data.csv', 'w') as f:
    # using csv.writer method from CSV package
    write = csv.writer(f)
    write.writerow(headers)
    write.writerows(data)
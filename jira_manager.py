from jira import JIRA

class JiraManager(object):
    def __init__(self, meta_jira):
        options = {'server': meta_jira['url']}
        self.jira = JIRA(options=options, basic_auth=(meta_jira['login'], meta_jira['pass']))

    def create_issue(self, cs):
        issue_dict = {
            'project': 'FP',
            'summary': 'Составить отчет по СИ ' + str(cs.src_code) + ' слою ' + cs.tgt_layer + '.',
            'description': 'Составить отчет по выверенным данным за период ' + cs.period + ' по слою ' + cs.tgt_layer + 'по СИ '
                           + cs.src_code + ' по пакету ' + str(cs.pkg_id) + '.',
            'issuetype': {'name': 'Task'}
        }
        print('new issue: {0}'.format(issue_dict))
        new_issue = self.jira.create_issue(fields=issue_dict)
        print(new_issue)
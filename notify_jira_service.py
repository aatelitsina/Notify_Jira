import schedule
import time
from dcm_conf import config
from db_manager import DBManager
from jira_manager import JiraManager


class NotifyJiraService(object):
    def __init__(self):
        meta_db = config['ora_dcm']
        connection_string = "{0}/{1}@{2}:{3}/{4}".format(meta_db['user'], meta_db['pwd'],
                                                         meta_db['host'], meta_db['port'],
                                                         meta_db['sid'])
        self.db_manager = DBManager(connection_string)
        self.jira = JiraManager(config['jira'])
        schedule.every(1).minute.do(self.__job)

    def run(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    def __job(self):
        for cs in self.db_manager.get_cs():
            print(cs)
            self.jira.create_issue(cs)
            self.db_manager.update_flag(cs)
            print('done upd')


if __name__ == "__main__":
    service = NotifyJiraService()
    service.run()
